package us.kbase.genomecomparison;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonServerMethod;
import us.kbase.common.service.JsonServerServlet;

//BEGIN_HEADER
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
//END_HEADER

/**
 * <p>Original spec-file module name: GenomeComparison</p>
 * <pre>
 * </pre>
 */
public class GenomeComparisonServer extends JsonServerServlet {
    private static final long serialVersionUID = 1L;

    //BEGIN_CLASS_HEADER
    private String configPath = null;
    private TaskHolder taskHolder = null;
    
    public void init(ServletConfig servletConfig) throws ServletException {
        configPath = servletConfig.getInitParameter("config_file");
    }
    
    private TaskHolder getTaskHolder() throws Exception {
    	if (taskHolder == null) {
    		File tempDir = new File(".");
    		File blastBin = null;
    		if (configPath != null) {
    			File f = new File(configPath);
    			if (f.exists()) {
    				Properties props = new Properties();
    				props.load(new FileInputStream(f));
    				if (props.containsKey("temp.dir"))
    					tempDir = new File(props.getProperty("temp.dir"));
    				if (props.containsKey("blast.bin"))
    					blastBin = new File(props.getProperty("blast.bin"));
        		} else {
        			System.out.println("Configuration file [" + new File(configPath).getAbsolutePath() + "] doesn't exist");
    			}
    		} else {
    			System.out.println("Configuration file was not set");
    		}
			taskHolder = new TaskHolder(tempDir, blastBin);
    	}
    	return taskHolder;
    }
    //END_CLASS_HEADER

    public GenomeComparisonServer() throws Exception {
        super("GenomeComparison");
        //BEGIN_CONSTRUCTOR
        //END_CONSTRUCTOR
    }

    /**
     * <p>Original spec-file function name: blast_proteomes</p>
     * <pre>
     * </pre>
     * @param   input   instance of type {@link us.kbase.genomecomparison.BlastProteomesParams BlastProteomesParams} (original type "blast_proteomes_params")
     * @return   parameter "job_id" of String
     */
    @JsonServerMethod(rpc = "GenomeComparison.blast_proteomes")
    public String blastProteomes(BlastProteomesParams input, AuthToken authPart) throws Exception {
        String returnVal = null;
        //BEGIN blast_proteomes
    	returnVal = getTaskHolder().addTask(input, authPart.toString());
        //END blast_proteomes
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: annotate_genome</p>
     * <pre>
     * </pre>
     * @param   input   instance of type {@link us.kbase.genomecomparison.AnnotateGenomeParams AnnotateGenomeParams} (original type "annotate_genome_params")
     * @return   parameter "job_id" of String
     */
    @JsonServerMethod(rpc = "GenomeComparison.annotate_genome")
    public String annotateGenome(AnnotateGenomeParams input, AuthToken authPart) throws Exception {
        String returnVal = null;
        //BEGIN annotate_genome
    	returnVal = getTaskHolder().addTask(input, authPart.toString());
        //END annotate_genome
        return returnVal;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: <program> <server_port>");
            return;
        }
        new GenomeComparisonServer().startupServer(Integer.parseInt(args[0]));
    }
}
