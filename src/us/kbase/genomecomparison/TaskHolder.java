package us.kbase.genomecomparison;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.Tuple3;
import us.kbase.common.service.UObject;
import us.kbase.workspaceservice.GetObjectParams;
import us.kbase.workspaceservice.ObjectData;
import us.kbase.workspaceservice.QueueJobParams;
import us.kbase.workspaceservice.SaveObjectParams;
import us.kbase.workspaceservice.SetJobStatusParams;
import us.kbase.workspaceservice.WorkspaceServiceClient;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaskHolder {
	private Map<String, Task> taskMap = new HashMap<String, Task>();
	private LinkedList<Task> taskQueue = new LinkedList<Task>();
	private Thread[] allThreads;
	private boolean needToStop = false;
	private final Object idleMonitor = new Object();
	private final File tempDir;
	private final File blastBin;
	
	private static final String wsUrl = "https://kbase.us/services/workspace/";
	
	public TaskHolder(int threadCount, File tempDir, File blastBin) {
		this.tempDir = tempDir;
		this.blastBin = blastBin;
		allThreads = new Thread[threadCount];
		for (int i = 0; i < allThreads.length; i++) {
			allThreads[i] = startNewThread(i);
		}
	}
	
	public synchronized String addTask(BlastProteomesParams params, String authToken) throws Exception {
		String jobId = createTaskJob(params, authToken);
		Task task = new Task(jobId, params, authToken);
		taskQueue.addLast(task);
		taskMap.put(task.getJobId(), task);
		synchronized (idleMonitor) {
			idleMonitor.notify();
		}
		return jobId;
	}
	
	private synchronized void removeTask(Task task) {
		taskMap.remove(task.getJobId());
	}
	
	public synchronized Task getTask(String jobId) {
		return taskMap.get(jobId);
	}
	
	private synchronized Task gainNewTask() {
		if (taskQueue.size() > 0) {
			Task ret = taskQueue.removeFirst();
			return ret;
		}
		return null;
	}

	private void runTask(Task task) {
		String token = task.getAuthToken();
		try {
			changeTaskState(task, "running", token, null);
			List<InnerFeature> features1 = extractProteome(task.getParams().getGenome1ws(), 
					task.getParams().getGenome1id(), token);
			Map<String, String> proteome1 = featuresToProtMap(features1);
			List<InnerFeature> features2 = extractProteome(task.getParams().getGenome2ws(), 
					task.getParams().getGenome2id(), token);
			Map<String, String> proteome2 = featuresToProtMap(features2);
			final Map<String, List<InnerHit>> data1 = new LinkedHashMap<String, List<InnerHit>>();
		    final Map<String, List<InnerHit>> data2 = new LinkedHashMap<String, List<InnerHit>>();
			String maxEvalue = task.getParams().getMaxEvalue() == null ? "1e-10" : task.getParams().getMaxEvalue();
			long time = System.currentTimeMillis();
			BlastStarter.run(tempDir, proteome1, proteome2, blastBin, maxEvalue, new BlastStarter.ResultCallback() {
				@Override
				public void proteinPair(String name1, String name2, double ident,
						int alnLen, int mismatch, int gapopens, int qstart, int qend,
						int tstart, int tend, String eval, double bitScore) {
					InnerHit h = new InnerHit().withId1(name1).withId2(name2).withScore(bitScore);
					List<InnerHit> l1 = data1.get(name1);
					if (l1 == null) {
						l1 = new ArrayList<InnerHit>();
						data1.put(name1, l1);
					}
					l1.add(h);
					List<InnerHit> l2 = data2.get(name2);
					if (l2 == null) {
						l2 = new ArrayList<InnerHit>();
						data2.put(name2, l2);
					}
					l2.add(h);
				}
			});
			Comparator<InnerHit> hcmp = new Comparator<InnerHit>() {
				@Override
				public int compare(InnerHit o1, InnerHit o2) {
					int ret = Double.compare(o2.getScore(), o1.getScore());
					if (ret == 0) {
						if (o1.getPercentOfBestScore() != null && o2.getPercentOfBestScore() != null) {
							ret = Long.compare(o2.getPercentOfBestScore(), o1.getPercentOfBestScore());
						}
					}
					return ret;
				}
			};
			Double subBbhPercentParam = task.getParams().getSubBbhPercent();
			double subBbhPercent = subBbhPercentParam == null ? 90 : subBbhPercentParam;
			for (Map.Entry<String, List<InnerHit>> entry : data1.entrySet()) 
				Collections.sort(entry.getValue(), hcmp);
			for (Map.Entry<String, List<InnerHit>> entry : data2.entrySet()) 
				Collections.sort(entry.getValue(), hcmp);
			for (Map.Entry<String, List<InnerHit>> entry : data1.entrySet()) {
				List<InnerHit> l = entry.getValue();
				double best1 = l.get(0).getScore();
				for (InnerHit h : l) {
					double best2 = getBestScore(h.getId2(), data2);
					h.setPercentOfBestScore(Math.round(h.getScore() * 100.0 / Math.max(best1, best2) + 1e-6));
				}
				for (int pos = l.size() - 1; pos > 0; pos--) 
					if (l.get(pos).getPercentOfBestScore() < subBbhPercent)
						l.remove(pos);
				Collections.sort(entry.getValue(), hcmp);
			}
			for (Map.Entry<String, List<InnerHit>> entry : data2.entrySet()) {
				List<InnerHit> l = entry.getValue();
				double best2 = l.get(0).getScore();
				for (InnerHit h : l) {
					double best1 = getBestScore(h.getId1(), data1);
					h.setPercentOfBestScore(Math.round(h.getScore() * 100.0 / Math.max(best1, best2) + 1e-6));
				}
				for (int pos = l.size() - 1; pos > 0; pos--) 
					if (l.get(pos).getPercentOfBestScore() < subBbhPercent)
						l.remove(pos);
				Collections.sort(entry.getValue(), hcmp);
			}
			List<String> prot1names = new ArrayList<String>();
			Map<String, Long> prot1map = new HashMap<String, Long>();
			linkedMapToPos(proteome1, prot1names, prot1map);
			List<String> prot2names = new ArrayList<String>();
			Map<String, Long> prot2map = new HashMap<String, Long>();
			linkedMapToPos(proteome2, prot2names, prot2map);
			List<List<Tuple3<Long, Long, Long>>> data1new = new ArrayList<List<Tuple3<Long, Long, Long>>>();
			for (String prot1name : prot1names) {
				List<Tuple3<Long, Long, Long>> hits = new ArrayList<Tuple3<Long, Long, Long>>();
				data1new.add(hits);
				List<InnerHit> ihits = data1.get(prot1name);
				if (ihits == null)
					continue;
				for (InnerHit ih : ihits) {
					Tuple3<Long, Long, Long> h = new Tuple3<Long, Long, Long>()
							.withE1(prot2map.get(ih.getId2())).withE2(Math.round(ih.getScore() * 100))
							.withE3(ih.getPercentOfBestScore());
					hits.add(h);
				}
			}
			List<List<Tuple3<Long, Long, Long>>> data2new = new ArrayList<List<Tuple3<Long, Long, Long>>>();
			for (String prot2name : prot2names) {
				List<Tuple3<Long, Long, Long>> hits = new ArrayList<Tuple3<Long, Long, Long>>();
				data2new.add(hits);
				List<InnerHit> ihits = data2.get(prot2name);
				if (ihits == null)
					continue;
				for (InnerHit ih : ihits) {
					Tuple3<Long, Long, Long> h = new Tuple3<Long, Long, Long>()
							.withE1(prot1map.get(ih.getId1())).withE2(Math.round(ih.getScore() * 100))
							.withE3(ih.getPercentOfBestScore());
					hits.add(h);
				}
			}
			ProteomeComparison res = new ProteomeComparison()
				.withSubBbhPercent(subBbhPercent)
				.withGenome1ws(task.getParams().getGenome1ws())
				.withGenome1id(task.getParams().getGenome1id())
				.withGenome2ws(task.getParams().getGenome2ws())
				.withGenome2id(task.getParams().getGenome2id())
				.withProteome1names(prot1names).withProteome1map(prot1map)
				.withProteome2names(prot2names).withProteome2map(prot2map)
				.withData1(data1new).withData2(data2new);
			saveResult(task.getParams().getOutputWs(), task.getParams().getOutputId(), token, res);
			changeTaskState(task, "done", token, null);
			time = System.currentTimeMillis() - time;
			//System.out.println("Time: " + time + " ms.");
		}catch(Throwable e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			pw.close();
			try {
				changeTaskState(task, "done", token, sw.toString());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
	}
	
	private static Map<String, String> featuresToProtMap(List<InnerFeature> features) {
		Map<String, String> ret = new LinkedHashMap<String, String>();
		for (InnerFeature inf : features)
			ret.put(inf.protName, inf.seq);
		return ret;
	}
	
	private static void linkedMapToPos(Map<String, String> linked, List<String> arr, 
			Map<String, Long> posMap) {
		for (String name: linked.keySet()) {
			long pos = arr.size();
			arr.add(name);
			posMap.put(name, pos);
		}
	}
	
	private static double getBestScore(String name, Map<String, List<InnerHit>> data) {
		List<InnerHit> l = data.get(name);
		if (l == null || l.isEmpty())
			return 0;
		return l.get(0).getScore();
	}

	public static WorkspaceServiceClient createWsClient(String token) throws Exception {
		WorkspaceServiceClient ret = new WorkspaceServiceClient(new URL(wsUrl), new AuthToken(token));
		ret.setAuthAllowedForHttp(true);
		return ret;
	}
	
	private String createTaskJob(BlastProteomesParams params, String token) throws Exception {
		Map<String, String> jobData = createJobDataMap(params);
		String ret = createWsClient(token).queueJob(new QueueJobParams().withAuth(token)
				.withQueuecommand("GenomeComparison.blast_proteomes")
				.withJobdata(jobData).withType("blastp")).getId();
		//System.out.println("Task was created");
		return ret;
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> createJobDataMap(BlastProteomesParams params) {
		Map<String, String> jobData = UObject.transformObjectToObject(params, Map.class);
		jobData.put("sub_bbh_percent", "" + jobData.get("sub_bbh_percent"));
		return jobData;
	}
	
	private void changeTaskState(Task task, String state, String token, String errorMessage) throws Exception {
		if ((!state.equals("running")) && (!state.equals("done")))
			throw new IllegalStateException("Unknown job state: " + state);
		Map<String, String> jobData = createJobDataMap(task.getParams());
		if (errorMessage != null)
			jobData.put("error", errorMessage);
		createWsClient(token).setJobStatus(new SetJobStatusParams().withAuth(token)
				.withStatus(state).withJobid(task.getJobId())
				.withJobdata(jobData));
		//System.out.println("Task state was changed: " + state);
		//if (errorMessage != null)
		//	System.out.println(errorMessage);
	}
	
	@SuppressWarnings("unchecked")
	private List<InnerFeature> extractProteome(String ws, String genomeId, String token) throws Exception {
		Map<String, Object> genome = (Map<String, Object>)createWsClient(token).getObject(
				new GetObjectParams().withAuth(token).withWorkspace(ws)
				.withId(genomeId).withType("Genome")).getData();
		List<Map<String, Object>> features = (List<Map<String, Object>>)genome.get("features");
		List<InnerFeature> ret = new ArrayList<InnerFeature>();
		for (Map<String, Object> feature : features) {
			String type = "" + feature.get("type");
			if (!type.equals("CDS"))
				continue;
			InnerFeature inf = new InnerFeature();
			inf.protName = "" + feature.get("id");
			inf.seq = "" + feature.get("protein_translation");
			List<Object> location = ((List<List<Object>>)feature.get("location")).get(0);
			inf.contigName = "" + location.get(0);
			int realStart = (Integer)location.get(1);
			String dir = "" + location.get(2);
			int len = (Integer)location.get(3);
			inf.start = dir.equals("+") ? realStart : (realStart - len);
			inf.stop = dir.equals("+") ? (realStart + len) : realStart;
			ret.add(inf);
		}
		Collections.sort(ret, new Comparator<InnerFeature>() {
			@Override
			public int compare(InnerFeature o1, InnerFeature o2) {
				int ret = o1.contigName.compareTo(o2.contigName);
				if (ret == 0) {
					ret = Integer.compare(o1.start, o2.start);
					if (ret == 0)
						ret = Integer.compare(o1.stop, o2.stop);
				}
				return ret;
			}
		});
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	private void saveResult(String ws, String id, String token, ProteomeComparison res) throws Exception {
		/*File dir = new File(id);
		File f = new File(dir, "cmp.json");
		new ObjectMapper().writeValue(f, res);
		ComparisonImage.saveImage(res, 25, new File(dir, "cmp.png"));
		*/
		ObjectData data = new ObjectData();
		data.getAdditionalProperties().putAll(UObject.transformObjectToObject(res, Map.class));
		createWsClient(token).saveObject(new SaveObjectParams().withAuth(token).withWorkspace(ws)
				.withType("ProteomeComparison").withId(id).withData(data));
	}
	
	public void stopAllThreads() {
		needToStop = true;
		for (Thread t : allThreads)
			t.interrupt();
	}
	
	private Thread startNewThread(final int num) {
		Thread ret = new Thread(
				new Runnable() {
					@Override
					public void run() {
						while (!needToStop) {
							Task task = gainNewTask();
							if (task != null) {
								runTask(task);
								removeTask(task);
							} else {
								int seconds = 55 + (int)(10 * Math.random());
								synchronized (idleMonitor) {
									try {
										idleMonitor.wait(TimeUnit.SECONDS.toMillis(seconds));
									} catch (InterruptedException e) {
										if (!needToStop)
											e.printStackTrace();
									}
								}
							}
						}
						System.out.println("Task thread " + (num + 1) + " was stoped");
					}
				},"Task thread " + (num + 1));
		ret.start();
		return ret;
	}

	private static class InnerHit {

		private String id1;
		private String id2;
		private Double score;
		private Long percentOfBestScore;

		public String getId1() {
			return id1;
		}

		public InnerHit withId1(String id1) {
			this.id1 = id1;
			return this;
		}

		public String getId2() {
			return id2;
		}

		public InnerHit withId2(String id2) {
			this.id2 = id2;
			return this;
		}

		public Double getScore() {
			return score;
		}

		public InnerHit withScore(Double score) {
			this.score = score;
			return this;
		}

		public Long getPercentOfBestScore() {
			return percentOfBestScore;
		}

		public void setPercentOfBestScore(Long percentOfBestScore) {
			this.percentOfBestScore = percentOfBestScore;
		}
	}
	
	private static class InnerFeature {
		String protName;
		String seq;
		String contigName;
		int start;
		int stop;
	}
}