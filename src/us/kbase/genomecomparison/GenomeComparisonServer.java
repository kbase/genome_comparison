package us.kbase.genomecomparison;

import java.util.List;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonServerMethod;
import us.kbase.common.service.JsonServerServlet;

//BEGIN_HEADER
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
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
    private TaskHolder taskHolder = null;

    public void init(ServletConfig servletConfig) throws ServletException {
    }
    
    private TaskHolder getTaskHolder() throws Exception {
    	if (taskHolder == null) {
			taskHolder = new TaskHolder(GenomeCmpConfig.loadConfig());
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

    /**
     * <p>Original spec-file function name: get_ncbi_genome_names</p>
     * <pre>
     * </pre>
     * @return   instance of list of String
     */
    @JsonServerMethod(rpc = "GenomeComparison.get_ncbi_genome_names")
    public List<String> getNcbiGenomeNames() throws Exception {
        List<String> returnVal = null;
        //BEGIN get_ncbi_genome_names
        returnVal = ContigSetUploadServlet.getNcbiGenomeNames();
        //END get_ncbi_genome_names
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: import_ncbi_genome</p>
     * <pre>
     * </pre>
     * @param   input   instance of type {@link us.kbase.genomecomparison.ImportNcbiGenomeParams ImportNcbiGenomeParams} (original type "import_ncbi_genome_params")
     */
    @JsonServerMethod(rpc = "GenomeComparison.import_ncbi_genome")
    public void importNcbiGenome(ImportNcbiGenomeParams input, AuthToken authPart) throws Exception {
        //BEGIN import_ncbi_genome
    	ContigSetUploadServlet.importNcbiGenome(input.getGenomeName(), input.getOutGenomeWs(), input.getOutGenomeId(), authPart.toString());
        //END import_ncbi_genome
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: <program> <server_port>");
            return;
        }
        new GenomeComparisonServer().startupServer(Integer.parseInt(args[0]));
    }
}
