package us.kbase.genomecomparison.test;

import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;

import us.kbase.genomeannotation.GenomeAnnotationClient;
import us.kbase.genomeannotation.GenomeTO;
import us.kbase.genomecomparison.FastaReader;

public class AnnotationHighLoadTester {
    private static final String gaUrl = "https://kbase.us/services/genome_annotation";
	private static GenomeTO genome = null;
	
	public static void main(String[] args) {
		for (int i = 0; i < 6; i++)
			runAnnotation(i + 1);
	}
	
	private static synchronized GenomeTO getDna() throws Exception {
		if (genome != null)
			return genome;
		System.out.println("Loading genome from NCBI");
		String[] nameAndFile = {"Shewanella_W3_18_1_uid58341", "NC_008750"};
		String genomeName = nameAndFile[0];
		//Map<String, Object> genomeData = loadWs1Genome(genomeId);
		String sourceId = nameAndFile[1];
		String contigSetId = genomeName + ".contigset";
		String dnaUrl = "ftp://ftp.ncbi.nlm.nih.gov/genomes/Bacteria/" + genomeName + "/" + sourceId + ".fna";
		FastaReader fr = new FastaReader(new InputStreamReader(new URL(dnaUrl).openStream()));
		String seq = fr.read()[1];
		System.out.println("\tsequence length: " + seq.length());
		us.kbase.genomeannotation.Contig contig = new us.kbase.genomeannotation.Contig()
			.withId(contigSetId + "." + sourceId).withDna(seq);
		genome = new GenomeTO().withContigs(Arrays.asList(contig)).withDomain("Bacteria")
				.withFeatures(Collections.<us.kbase.genomeannotation.Feature>emptyList())
				.withGeneticCode(11L).withId(genomeName).withScientificName(genomeName.replace('_', ' '))
				.withSource(dnaUrl).withSourceId("NCBI");
		System.out.println("\tdone.");
		return genome;
	}
	
	private static void runAnnotation(int number) {
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				long time = System.currentTimeMillis();
				try {
					System.out.println("Thread [" + Thread.currentThread().getName() + "]: before annotation call");
					GenomeAnnotationClient gc = new GenomeAnnotationClient(new URL(gaUrl));
					GenomeTO gto = gc.annotateGenome(getDna());
					time = System.currentTimeMillis() - time;
					System.out.println("Thread [" + Thread.currentThread().getName() + "]: " + gto.getFeatures().size() + " features found (time=" + time + ")");
				} catch (Exception ex) {
					time = System.currentTimeMillis() - time;
					System.out.println("Thread [" + Thread.currentThread().getName() + "]: error=" + ex.getMessage() + " (time=" + time + ")");
					ex.printStackTrace();
				}
			}
		}, "t" + number).start();
	}
}
