package us.kbase.genomecomparison.test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Assert;

import org.easymock.EasyMockSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import us.kbase.common.service.JsonClientException;
import us.kbase.genomecomparison.GenomeCmpConfig;
import us.kbase.genomecomparison.JobStatuses;
import us.kbase.genomecomparison.ObjectStorage;
import us.kbase.genomecomparison.TaskHolder;
import us.kbase.genomecomparison.util.DbConn;
import us.kbase.userandjobstate.InitProgress;
import us.kbase.userandjobstate.Results;

public class TaskHolderTest extends EasyMockSupport {
	private static File tmpDir = new File("temp");
	
	@BeforeClass
	@AfterClass
	public static void dropDatabase() throws Exception {
		if (!tmpDir.exists())
			tmpDir.mkdir();
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		File dbDir = new File(tmpDir, TaskHolder.DERBY_DB_NAME);
		try {
			DriverManager.getConnection("jdbc:derby:" + dbDir.getParent() + "/" + dbDir.getName() + ";shutdown=true");
		} catch (Exception ignore) {}
		delete(dbDir);
	}
	
	private static void delete(File fileOrDir) {
		if (fileOrDir.isDirectory()) {
			for (File sub : fileOrDir.listFiles())
				delete(sub);
		}
		fileOrDir.delete();
	}
	
	@Test
	public void testGood() throws Exception {
		String token = "secret";
		String jobId = "job123";
		ObjectStorage obst = createStrictMock(ObjectStorage.class);
		JobStatuses jbst = createStrictMock(JobStatuses.class);
		expect(jbst.createAndStartJob(eq(token), eq("queued"), eq("descr"), 
				anyObject(InitProgress.class), isNull(String.class))).andReturn(jobId);
		jbst.updateJob(eq(jobId), eq(token), eq("running"), isNull(String.class));
		//expectLastCall();
		jbst.completeJob(eq(jobId), eq(token), eq("done"), isNull(String.class), anyObject(Results.class));
		final TaskHolder[] th = {null};
		final boolean[] complete = {false};
		expectLastCall().andDelegateTo(new JobStatuses() {
			@Override
			public void updateJob(String job, String token, String status,
					String estComplete) throws IOException, JsonClientException {
			}
			@Override
			public String createAndStartJob(String token, String status, String desc,
					InitProgress progress, String estComplete) throws IOException, JsonClientException {
				return null;
			}
			@Override
		    public void completeJob(String job, String token, String status, String error, Results res) 
		    		throws IOException, JsonClientException {
				complete[0] = true;
		    }
		});
		replayAll();
		th[0] = new TaskHolder(new GenomeCmpConfig(tmpDir, null, null, obst, jbst));
		Assert.assertEquals(jobId, th[0].addTaskForTest(new TestTask("something-saved", null), token));
		while (!complete[0]) {
			Thread.sleep(100);
		}
		th[0].stopAllThreads();
		verifyAll();
		Assert.assertEquals((Integer)0, TaskHolder.getDbConnection(tmpDir).collect(
				"select count(*) from " + TaskHolder.QUEUE_TABLE_NAME, new DbConn.SqlLoader<Integer>() {
			@Override
			public Integer collectRow(ResultSet rs) throws SQLException {
				return rs.getInt(1);
			}
		}).get(0));
	}

	@Test
	public void testBad() throws Exception {
		String token = "secret";
		String jobId = "job123";
		String errorMsg = "Super error!";
		ObjectStorage obst = createStrictMock(ObjectStorage.class);
		JobStatuses jbst = createStrictMock(JobStatuses.class);
		expect(jbst.createAndStartJob(eq(token), eq("queued"), eq("descr"), 
				anyObject(InitProgress.class), isNull(String.class))).andReturn(jobId);
		jbst.updateJob(eq(jobId), eq(token), eq("running"), isNull(String.class));
		jbst.completeJob(eq(jobId), eq(token), eq("Error: " + errorMsg), anyObject(String.class), anyObject(Results.class));
		final TaskHolder[] th = {null};
		final boolean[] complete = {false};
		expectLastCall().andDelegateTo(new JobStatuses() {
			@Override
			public void updateJob(String job, String token, String status,
					String estComplete) throws IOException, JsonClientException {
			}
			@Override
			public String createAndStartJob(String token, String status, String desc,
					InitProgress progress, String estComplete) throws IOException, JsonClientException {
				return null;
			}
			@Override
		    public void completeJob(String job, String token, String status, String error, Results res) 
		    		throws IOException, JsonClientException {
				complete[0] = true;
		    }
		});
		replayAll();
		th[0] = new TaskHolder(new GenomeCmpConfig(tmpDir, null, null, obst, jbst));
		Assert.assertEquals(jobId, th[0].addTaskForTest(new TestTask("something-saved", new Runnable() {
			@Override
			public void run() {
				throw new IllegalStateException("Super error!");
			}
		}), token));
		while (!complete[0]) {
			Thread.sleep(100);
		}
		th[0].stopAllThreads();
		verifyAll();
		Assert.assertEquals((Integer)0, TaskHolder.getDbConnection(tmpDir).collect(
				"select count(*) from " + TaskHolder.QUEUE_TABLE_NAME, new DbConn.SqlLoader<Integer>() {
			@Override
			public Integer collectRow(ResultSet rs) throws SQLException {
				return rs.getInt(1);
			}
		}).get(0));
	}

	public static class TestTask implements Runnable {
		private String innerParam;
		private Runnable innerRunnable;
		
		public TestTask() {
		}
		
		public TestTask(String param, Runnable runnable) {
			this.innerParam = param;
			this.innerRunnable = runnable;
		}
		
		public String getInnerParam() {
			return innerParam;
		}
		
		public void setInnerParam(String innerParam) {
			this.innerParam = innerParam;
		}
		
		@Override
		public void run() {
			if (innerRunnable != null)
				innerRunnable.run();
		}
	}
}
