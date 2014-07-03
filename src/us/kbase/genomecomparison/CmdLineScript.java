package us.kbase.genomecomparison;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;

import org.ini4j.Ini;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import us.kbase.userandjobstate.Results;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CmdLineScript {
	private static final String SERVICE_NAME = "genome_comparison";
	private static final String TOKEN_ENV_VAR_NAME = "KB_AUTH_TOKEN";
	private static final String KB_TOP_ENV_VAR_NAME = "KB_TOP";
	private static final String DEFAULT_CONFIG_REL_PATH = "services/" + SERVICE_NAME + "/deploy.cfg";
    private static final int MAX_ERROR_MESSAGE_LEN = 190;

	public static void main(String[] args) throws Exception {
		Args parsedArgs = new Args();
		CmdLineParser parser = new CmdLineParser(parsedArgs);
		parser.setUsageWidth(85);
		if (args.length == 0 || (args.length == 1 && (args[0].equals("-h") || args[0].equals("--help")))) {
            parser.parseArgument("no.spec");
            showUsage(parser, null, System.out);
            return;
		}
		try {
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
        	String message = e.getMessage();
            showUsage(parser, message, System.err);
            return;
        }
		Class<?> type = Class.forName(parsedArgs.type);
		String paramsJson = parsedArgs.params;
		Object params = new ObjectMapper().readValue(paramsJson, type);
		String auth = parsedArgs.auth;
		if (auth == null)
			auth = System.getenv(TOKEN_ENV_VAR_NAME);
		String configPath = parsedArgs.config;
		File configFile;
		if (configPath == null) {
			String kbTop = System.getenv(KB_TOP_ENV_VAR_NAME);
			if (kbTop == null)
				kbTop = "/kb/deployment";
			configFile = new File(kbTop, DEFAULT_CONFIG_REL_PATH);
		} else {
			configFile = new File(configPath);
		}
		Ini ini = new Ini(configFile);
		Map<String, String> config = ini.get(SERVICE_NAME);
		runTask(new Task(parsedArgs.jobid, params, auth, parsedArgs.outref), new GenomeCmpConfig(config));
	}
	
	private static void runTask(Task task, GenomeCmpConfig config) {
		String token = task.getAuthToken();
		try {
			changeTaskStateIntoRunning(task, token, config);
			Object params = task.getParams();
			if (params instanceof BlastProteomesParams) {
				BlastProteomes.run(token, (BlastProteomesParams)params, config);
			} else if (params instanceof AnnotateGenomeParams) {
				AnnotateGenome.run(token, (AnnotateGenomeParams)params, config);
			} else if (params instanceof Runnable) {
				((Runnable)params).run();
			} else {
				throw new IllegalStateException("Unsupported task type: " + params.getClass().getName());
			}
			completeTaskState(task, token, null, null, config);
		} catch (Throwable e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			pw.close();
			try {
				String errMsg = null;
				if (e.getMessage() == null) {
					errMsg = e.getClass().getSimpleName();
				} else {
					errMsg = "Error: " + e.getMessage();
				}
				if (errMsg.length() > MAX_ERROR_MESSAGE_LEN)
					errMsg = errMsg.substring(0, MAX_ERROR_MESSAGE_LEN - 3) + "...";
				completeTaskState(task, token, errMsg, sw.toString(), config);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	private static void changeTaskStateIntoRunning(Task task, String token, GenomeCmpConfig config) throws Exception {
		config.getJobStatuses().updateJob(task.getJobId(), token, "running", null);
	}

	private static void completeTaskState(Task task, String token, String errorMessage, String errorStacktrace, 
			GenomeCmpConfig config) throws Exception {
		if (errorMessage == null) {
			config.getJobStatuses().completeJob(task.getJobId(), token, "done", null, 
					new Results().withWorkspaceurl(config.getWsUrl()).withWorkspaceids(
							Arrays.asList(task.getOutRef())));
		} else {
			config.getJobStatuses().completeJob(task.getJobId(), token, errorMessage, 
					errorStacktrace, new Results()); 
		}
	}

	private static void showUsage(CmdLineParser parser, String message, PrintStream out) {
		if (message != null)
			out.println(message);
		out.println("Program runs job client part of ProteinComparison service.");
		out.println("Usage: <program> options...");
		out.println("Usage: <program> {-h|--help}     - to see this help");
		parser.printUsage(out);
	}

	public static class Args {
		@Option(name="-t",required=true,usage="Type of input parameters object", metaVar="<type>")
		String type;

		@Option(name="-p",required=true,usage="Input parameters object (in JSON)", metaVar="<params>")
		String params;

		@Option(name="-j",required=true,usage="Job status service id of job created for this execution", metaVar="<jobid>")
		String jobid;
		
		@Option(name="-o",required=true,usage="Workspace reference to resulting output object", metaVar="<outref>")		
		String outref;

		@Option(name="-a", usage="User token used for writing results into workspace (optional, in case of AWE use private env-var " + 
		TOKEN_ENV_VAR_NAME + " in job template instead of -t)", metaVar="<auth>")
		String auth = null;

		@Option(name="-c", usage="Path to service configuration file (optional, default value is $" + KB_TOP_ENV_VAR_NAME + "/" + 
		DEFAULT_CONFIG_REL_PATH + ")", metaVar="<config>")
		String config = null;
	}
}
