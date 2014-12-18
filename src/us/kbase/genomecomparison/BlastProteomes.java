package us.kbase.genomecomparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import us.kbase.common.service.Tuple3;
import us.kbase.common.service.Tuple4;
import us.kbase.common.service.UObject;
import us.kbase.kbasegenomes.Feature;
import us.kbase.kbasegenomes.Genome;
import us.kbase.workspace.ObjectIdentity;
import us.kbase.workspace.ObjectSaveData;
import us.kbase.workspace.SaveObjectsParams;

public class BlastProteomes {
	
	public static void run(String token, BlastProteomesParams params, GenomeCmpConfig cfg) throws Exception {
		List<InnerFeature> features1 = extractProteome(params.getGenome1ws(), 
				params.getGenome1id(), token, cfg.getObjectStorage());
		Map<String, String> proteome1 = featuresToProtMap(features1);
		List<InnerFeature> features2 = extractProteome(params.getGenome2ws(), 
				params.getGenome2id(), token, cfg.getObjectStorage());
		Map<String, String> proteome2 = featuresToProtMap(features2);
		final Map<String, List<InnerHit>> data1 = new LinkedHashMap<String, List<InnerHit>>();
		final Map<String, List<InnerHit>> data2 = new LinkedHashMap<String, List<InnerHit>>();
		String maxEvalue = params.getMaxEvalue() == null ? "1e-10" : params.getMaxEvalue();
		BlastStarter.run(cfg.getTempDir(), proteome1, proteome2, cfg.getBlastBin(), maxEvalue, new BlastStarter.ResultCallback() {
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
						ret = Utils.compare(o2.getPercentOfBestScore(), o1.getPercentOfBestScore());
					}
				}
				return ret;
			}
		};
		Double subBbhPercentParam = params.getSubBbhPercent();
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
			.withMaxEvalue(maxEvalue)
			.withGenome1ref(params.getGenome1ws() + "/" + params.getGenome1id())
			.withGenome2ref(params.getGenome2ws() + "/" + params.getGenome2id())
			.withProteome1names(prot1names)
			.withProteome1map(prot1map)
			.withProteome2names(prot2names)
			.withProteome2map(prot2map)
			.withData1(data1new)
			.withData2(data2new);
		saveResult(params.getOutputWs(), params.getOutputId(), token, res, cfg.getObjectStorage());
	}

	private static Map<String, String> featuresToProtMap(List<InnerFeature> features) {
		Map<String, String> ret = new LinkedHashMap<String, String>();
		for (InnerFeature inf : features) {
			if (inf.seq == null || inf.seq.trim().isEmpty())
				continue;
			ret.put(inf.protName, inf.seq);
		}
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

	private static List<InnerFeature> extractProteome(String ws, String genomeId, String token,
			ObjectStorage objectStorage) throws Exception {
		UObject genomeObj = objectStorage.getObjects(token,
				Arrays.asList(new ObjectIdentity().withRef(ws + "/" + genomeId))).get(0).getData();
		Genome genome = genomeObj.asClassInstance(Genome.class);
		List<InnerFeature> ret = new ArrayList<InnerFeature>();
		for (Feature feature : genome.getFeatures()) {
			String type = feature.getType();
			if (!type.equals("CDS"))
				continue;
			InnerFeature inf = new InnerFeature();
			inf.protName = feature.getId();
			inf.seq = feature.getProteinTranslation();
			if (inf.seq == null)
				continue;
			inf.seq = inf.seq.trim();
			if (inf.seq.isEmpty())
				continue;
			Tuple4<String, Long, String, Long> location = feature.getLocation().get(0);
			inf.contigName = location.getE1();
			int realStart = (int)(long)location.getE2();
			String dir = location.getE3();
			int len = (int)(long)location.getE4();
			inf.start = dir.equals("+") ? realStart : (realStart - len);
			inf.stop = dir.equals("+") ? (realStart + len) : realStart;
			ret.add(inf);
		}
		Collections.sort(ret, new Comparator<InnerFeature>() {
			@Override
			public int compare(InnerFeature o1, InnerFeature o2) {
				int ret = o1.contigName.compareTo(o2.contigName);
				if (ret == 0) {
					ret = Utils.compare(o1.start, o2.start);
					if (ret == 0)
						ret = Utils.compare(o1.stop, o2.stop);
				}
				return ret;
			}
		});
		return ret;
	}

	private static void saveResult(String ws, String id, String token, ProteomeComparison res,
			ObjectStorage objectStorage) throws Exception {
		ObjectSaveData data = new ObjectSaveData().withData(new UObject(res)).withType("GenomeComparison.ProteomeComparison");
		try {
			long objid = Long.parseLong(id);
			data.withObjid(objid);
		} catch (NumberFormatException ex) {
			data.withName(id);
		}
		objectStorage.saveObjects(token, new SaveObjectsParams().withWorkspace(ws).withObjects(
				Arrays.asList(data)));
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
