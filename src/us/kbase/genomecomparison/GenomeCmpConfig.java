package us.kbase.genomecomparison;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

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
	private File tempDir;
	private File blastBin;
	private String wsUrl; 
	private ObjectStorage objectStorage;
	private JobStatuses jobStatuses;

	private static final String defaultWsUrl = "https://kbase.us/services/ws/";
	private static final String defaultJssUrl = "https://kbase.us/services/userandjobstate/";

	public GenomeCmpConfig() {
		this(null, null, null, null);
	}

	public GenomeCmpConfig(Map<String, String> cfgMap) {
		this(asFile(cfgMap.get("temp.dir")), asFile(cfgMap.get("blast.bin")), cfgMap.get("ws.url"), cfgMap.get("jss.url"));
	}
	
	public GenomeCmpConfig(File tempDir, File blastBin, final String wsUrl, final String jssUrl) {
		this(tempDir, blastBin, wsUrl, new ObjectStorage() {
			@Override
			public List<ObjectData> getObjects(String token, List<ObjectIdentity> objectIds) throws Exception {
				return createWsClient(getNotNull(wsUrl, defaultWsUrl), token).getObjects(objectIds);
			}
			@Override
			public List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> saveObjects(
					String token, SaveObjectsParams params) throws Exception {
				return createWsClient(getNotNull(wsUrl, defaultWsUrl), token).saveObjects(params);
			}
		}, new JobStatuses() {
			@Override
			public String createAndStartJob(String token, String status, String desc,
					InitProgress progress, String estComplete) throws IOException, JsonClientException {
				return createJobClient(getNotNull(jssUrl, defaultJssUrl), token).createAndStartJob(token, status, desc, progress, estComplete);
			}
			@Override
			public void updateJob(String job, String token, String status, String estComplete) throws IOException, JsonClientException {
				createJobClient(getNotNull(jssUrl, defaultJssUrl), token).updateJob(job, token, status, estComplete);
			}
			@Override
			public void completeJob(String job, String token, String status,
					String error, Results res) throws IOException, JsonClientException {
				createJobClient(getNotNull(jssUrl, defaultJssUrl), token).completeJob(job, token, status, error, res);
			}
		});
	}
	
	public GenomeCmpConfig(File tempDir, File blastBin, String wsUrl, ObjectStorage objectStorage, JobStatuses jobStatuses) {
		this.tempDir = tempDir;
		this.blastBin = blastBin;
		this.wsUrl = getNotNull(wsUrl, defaultWsUrl);
		this.objectStorage = objectStorage;
		this.jobStatuses = jobStatuses;
	}
		
	private static String getNotNull(String val, String defVal) { 
		return val == null ? defVal : val;
	}
	
	private static File asFile(String path) {
		return path == null ? null : new File(path);
	}

	public static WorkspaceClient createWsClient(String token) throws Exception {
		return createWsClient(defaultWsUrl, token);
	}
	
	public static WorkspaceClient createWsClient(String wsUrl, String token) throws Exception {
		WorkspaceClient ret = new WorkspaceClient(new URL(wsUrl), new AuthToken(token));
		ret.setAuthAllowedForHttp(true);
		return ret;
	}

	public static UserAndJobStateClient createJobClient(String token) throws IOException, JsonClientException {
		return createJobClient(defaultJssUrl, token);
	}

	public static UserAndJobStateClient createJobClient(String jssUrl, String token) throws IOException, JsonClientException {
		try {
			UserAndJobStateClient ret = new UserAndJobStateClient(new URL(jssUrl), new AuthToken(token));
			ret.setAuthAllowedForHttp(true);
			return ret;
		} catch (TokenFormatException e) {
			throw new JsonClientException(e.getMessage(), e);
		} catch (UnauthorizedException e) {
			throw new JsonClientException(e.getMessage(), e);
		}
	}

	public File getTempDir() {
		return tempDir;
	}
	
	public File getBlastBin() {
		return blastBin;
	}
	
	public String getWsUrl() {
		return wsUrl;
	}
	
	public ObjectStorage getObjectStorage() {
		return objectStorage;
	}
	
	public JobStatuses getJobStatuses() {
		return jobStatuses;
	}
}
