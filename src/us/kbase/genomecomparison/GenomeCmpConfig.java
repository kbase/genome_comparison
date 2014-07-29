package us.kbase.genomecomparison;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.ini4j.Ini;

import us.kbase.auth.AuthToken;
import us.kbase.auth.TokenFormatException;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.UnauthorizedException;
import us.kbase.userandjobstate.InitProgress;
import us.kbase.userandjobstate.Results;
import us.kbase.userandjobstate.UserAndJobStateClient;
import us.kbase.workspace.ObjectData;
import us.kbase.workspace.ObjectIdentity;
import us.kbase.workspace.SaveObjectsParams;
import us.kbase.workspace.WorkspaceClient;

public class GenomeCmpConfig {
	private int threadCount;
	private File tempDir;
	private File blastBin;
	private ObjectStorage objectStorage;
	private JobStatuses jobStatuses;
	private String wsUrl;
    private String ujsUrl;

	private static String defWsUrl = "https://kbase.us/services/ws/";  // http://dev04.berkeley.kbase.us:7058
    private static String defUjsUrl = "https://kbase.us/services/userandjobstate/";
    public static final String PROP_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG";

	public static GenomeCmpConfig loadConfig() throws IOException {
		int threadCount = 1;
		File tempDir = new File(".");
		File blastBin = null;
		String wsUrl = defWsUrl;
		String ujsUrl = defUjsUrl;
    	String configPath = System.getProperty(PROP_KB_DEPLOYMENT_CONFIG);
    	if (configPath == null)
    		configPath = System.getenv(PROP_KB_DEPLOYMENT_CONFIG);
		if (configPath == null) {
			InputStream is = GenomeCmpConfig.class.getResourceAsStream("config_path.properties");
			try {
				Properties props = new Properties();
				props.load(is);
				configPath = props.getProperty("config_path");
				System.out.println("[genome_comparison] using configuration file: " + configPath);
				System.setProperty(PROP_KB_DEPLOYMENT_CONFIG, configPath);
			} catch (IOException ex) {
				ex.printStackTrace();
				throw new IllegalStateException("Error loading configuration: " + ex.getMessage(), ex);
			} finally {
				try { is.close(); } catch (Exception ignore) {}
			}
		}
		File f = new File(configPath);
		if (f.exists()) {
			Map<String, String> props = new Ini(new File(configPath)).get("genome_comparison");
			if (props == null)
				System.out.println("Can not load properties from configuration file: " + configPath);
			if (props.containsKey("thread.count"))
				threadCount = Integer.parseInt(props.get("thread.count"));
			if (props.containsKey("temp.dir"))
				tempDir = new File(props.get("temp.dir"));
			if (props.containsKey("blast.dir"))
				blastBin = new File(props.get("blast.dir"));
			if (props.containsKey("ws.url"))
				wsUrl = props.get("ws.url");
			if (props.containsKey("ujs.url"))
				ujsUrl = props.get("ujs.url");
		} else {
			throw new IOException("Configuration file [" + new File(configPath).getAbsolutePath() + "] doesn't exist");
		}
		return new GenomeCmpConfig(threadCount, tempDir, blastBin, wsUrl, ujsUrl);
	}

	public GenomeCmpConfig(int threadCount, File tempDir, File blastBin) {
		this(threadCount, tempDir, blastBin, defWsUrl, defUjsUrl);
	}
	
	public GenomeCmpConfig(int threadCount, File tempDir, File blastBin, final String wsUrl, final String ujsUrl) {
		this(threadCount, tempDir, blastBin, wsUrl, ujsUrl, new ObjectStorage() {
			@Override
			public List<ObjectData> getObjects(String token, List<ObjectIdentity> objectIds) throws Exception {
				return createWsClient(token, wsUrl).getObjects(objectIds);
			}
			@Override
			public List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> saveObjects(
					String token, SaveObjectsParams params) throws Exception {
				return createWsClient(token, wsUrl).saveObjects(params);
			}
		}, new JobStatuses() {
			@Override
			public String createAndStartJob(String token, String status, String desc,
					InitProgress progress, String estComplete) throws IOException, JsonClientException {
				return createJobClient(token, ujsUrl).createAndStartJob(token, status, desc, progress, estComplete);
			}
			@Override
			public void updateJob(String job, String token, String status, String estComplete) throws IOException, JsonClientException {
				createJobClient(token, ujsUrl).updateJob(job, token, status, estComplete);
			}
			@Override
			public void completeJob(String job, String token, String status,
					String error, Results res) throws IOException, JsonClientException {
				createJobClient(token, ujsUrl).completeJob(job, token, status, error, res);
			}
		});
	}
	
	public GenomeCmpConfig(int threadCount, File tempDir, File blastBin, String wsUrl, String ujsUrl, ObjectStorage objectStorage, JobStatuses jobStatuses) {
		this.threadCount = threadCount;
		this.tempDir = tempDir;
		this.blastBin = blastBin;
		this.wsUrl = wsUrl == null ? defWsUrl : wsUrl;
		this.ujsUrl = ujsUrl == null ? defUjsUrl : ujsUrl;
		this.objectStorage = objectStorage;
		this.jobStatuses = jobStatuses;
	}
	
	public String getWsUrl() {
		return wsUrl;
	}
	
	public String getJobSrvUrl() {
		return ujsUrl;
	}
	
	public static WorkspaceClient createWsClient(String token, String wsUrl) throws Exception {
		WorkspaceClient ret = new WorkspaceClient(new URL(wsUrl), new AuthToken(token));
		ret.setAuthAllowedForHttp(true);
		return ret;
	}

	public static WorkspaceClient createWsClient(String token) throws Exception {
		String wsUrl = loadConfig().getWsUrl();
		return createWsClient(token, wsUrl);
	}
	
	public static UserAndJobStateClient createJobClient(String token, String ujsUrl) throws IOException, JsonClientException {
		try {
			UserAndJobStateClient ret = new UserAndJobStateClient(new URL(ujsUrl), new AuthToken(token));
			ret.setAuthAllowedForHttp(true);
			return ret;
		} catch (TokenFormatException e) {
			throw new JsonClientException(e.getMessage(), e);
		} catch (UnauthorizedException e) {
			throw new JsonClientException(e.getMessage(), e);
		}
	}

	public static UserAndJobStateClient createJobClient(String token) throws IOException, JsonClientException {
		String ujsUrl = loadConfig().getJobSrvUrl();
		return createJobClient(token, ujsUrl);
	}

	public int getThreadCount() {
		return threadCount;
	}
	
	public File getTempDir() {
		return tempDir;
	}
	
	public File getBlastBin() {
		return blastBin;
	}
	
	public ObjectStorage getObjectStorage() {
		return objectStorage;
	}
	
	public JobStatuses getJobStatuses() {
		return jobStatuses;
	}
}
