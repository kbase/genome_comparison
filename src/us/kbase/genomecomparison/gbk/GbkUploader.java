package us.kbase.genomecomparison.gbk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple3;
import us.kbase.common.service.Tuple4;
import us.kbase.common.service.UObject;
import us.kbase.kbasegenomes.Contig;
import us.kbase.kbasegenomes.ContigSet;
import us.kbase.kbasegenomes.Feature;
import us.kbase.kbasegenomes.Genome;
import us.kbase.workspace.ObjectData;
import us.kbase.workspace.ObjectIdentity;
import us.kbase.workspace.ObjectSaveData;
import us.kbase.workspace.SaveObjectsParams;
import us.kbase.workspace.WorkspaceClient;

public class GbkUploader {

	public static void uploadGbk(List<File> files, String wsUrl, String wsName, String id, String token) throws Exception {
		final WorkspaceClient wc = new WorkspaceClient(new URL(wsUrl), new AuthToken(token));
		wc.setAuthAllowedForHttp(true);
		uploadGbk(files, new ObjectStorage() {
			
			@Override
			public List<Tuple11<Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> saveObjects(
					String authToken, SaveObjectsParams params) throws Exception {
				return wc.saveObjects(params);
			}
			
			@Override
			public List<ObjectData> getObjects(String authToken,
					List<ObjectIdentity> objectIds) throws Exception {
				return wc.getObjects(objectIds);
			}
		}, wsName, id, token);
	}
	
	public static void uploadGbk(List<File> files, ObjectStorage wc, String ws, String id, String token) throws Exception {
		final Map<String, Contig> contigMap = new LinkedHashMap<String, Contig>();
		final Genome genome = new Genome()
				.withComplete(1L).withDomain("Bacteria").withGeneticCode(11L).withId(id)
				.withNumContigs(1L).withSource("NCBI").withSourceId("NCBI");
		final List<Feature> features = new ArrayList<Feature>();
		final Set<String> usedFeatureIds = new HashSet<String>();
		final Map<String, Integer> generatedFeatureIds = new HashMap<String, Integer>();
		final Map<String, String> contigToOrgName = new HashMap<String, String>();
		final Map<String, String> contigToTaxonomy = new HashMap<String, String>();
		final Map<String, Boolean> contigToPlasmid = new HashMap<String, Boolean>();
		for (final File f : files) {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			try {
				GbkParser.parse(br, new GbkParsingParams(true), new GbkCallback() {
					@Override
					public void setGenome(String contigName, String genomeName, int taxId, String plasmid) throws Exception {
						if (contigToOrgName.get(contigName) == null)
							contigToOrgName.put(contigName, genomeName);
						genome.getAdditionalProperties().put("tax_id", taxId);
						contigToPlasmid.put(contigName, plasmid != null);
					}
					@Override
					public void addSeqPart(String contigName, int seqPartIndex, String seqPart,
							int commonLen) throws Exception {
						Contig contig = contigMap.get(contigName);
						if (contig == null) {
							contig = new Contig().withId(contigName).withName(contigName).withMd5("md5")
									.withSequence("");
							contigMap.put(contigName, contig);
						}
						contig.withSequence(contig.getSequence() + seqPart);
					}
					@Override
					public void addHeader(String contigName, String headerType, String value,
							List<GbkSubheader> items) throws Exception {
						if (headerType.equals("SOURCE")) {
							String genomeName = value;
							//genome.withScientificName(genomeName);
							contigToOrgName.put(contigName, genomeName);
							for (GbkSubheader sub : items) {
								if (sub.type.equals("ORGANISM")) {
									String taxPath = sub.getValue();
									String[] parts = taxPath.split("\n");
									taxPath = "";
									for (int i = 0; i < parts.length; i++) {
										if (i == 0 && parts[0].equals(genomeName))
											continue;
										if (taxPath.length() > 0)
											taxPath += " ";
										taxPath += parts[i];
									}
									if (taxPath.endsWith("."))
										taxPath = taxPath.substring(0, taxPath.length() - 1).trim();
									String fullPath = taxPath + "; " + genomeName;
									contigToTaxonomy.put(contigName, fullPath);
								}
							}
						}
					}
					@Override
					public void addFeature(String contigName, String featureType, int strand,
							int start, int stop, List<GbkLocation> locations,
							List<GbkQualifier> props) throws Exception {
						Feature f = null;
						if (featureType.equals("CDS")) {
							f = new Feature().withType("CDS");
						} else if (featureType.toUpperCase().endsWith("RNA")) {
							f = new Feature().withType("rna");
						}
						if (f == null)
							return;
						List<Tuple4<String, Long, String, Long>> locList = new ArrayList<Tuple4<String, Long, String, Long>>();
						for (GbkLocation loc : locations) {
							long realStart = loc.strand > 0 ? loc.start : loc.stop;
							String dir = loc.strand > 0 ? "+" : "-";
							long len = loc.stop + 1 - loc.start;
							locList.add(new Tuple4<String, Long, String, Long>().withE1(contigName)
									.withE2(realStart).withE3(dir).withE4(len));
						}
						f.withLocation(locList).withAnnotations(new ArrayList<Tuple3<String, String, Long>>());
						f.withAliases(new ArrayList<String>());
						for (GbkQualifier prop : props) {
							if (prop.type.equals("locus_tag")) {
								f.setId(prop.getValue());
							} else if (prop.type.equals("translation")) {
								String seq = prop.getValue();
								f.withProteinTranslation(seq).withProteinTranslationLength((long)seq.length());
							} else if (prop.type.equals("note")) {
								f.setFunction(prop.getValue());
							} else if (prop.type.equals("product")) {
								if (f.getFunction() == null)
									f.setFunction(prop.getValue());
							} else if (prop.type.equals("gene")) {
								if (f.getId() == null)
									f.setId(prop.getValue());
								f.getAliases().add(prop.getValue());
							} else if (prop.type.equals("protein_id")) {
								f.getAliases().add(prop.getValue());
							}
						}
						if (f.getId() == null) {
							Integer last = generatedFeatureIds.get(f.getType());
							if (last == null)
								last = 0;
							last++;
							f.setId(f.getType() + "." + last);
							generatedFeatureIds.put(f.getType(), last);
						}
						features.add(f);
						usedFeatureIds.add(f.getId());
					}
				});
			} finally {
				br.close();
			}
		}
		// Process all non-plasmids first
		for (String key : contigToOrgName.keySet()) {
			Boolean isPlasmid = contigToPlasmid.get(key);
			if (isPlasmid != null && isPlasmid)
				continue;
			genome.setScientificName(contigToOrgName.get(key));
			String taxonomy = contigToTaxonomy.get(key);
			if (taxonomy != null) {
				if (genome.getTaxonomy() != null && !genome.getTaxonomy().equals(taxonomy))
					System.err.println("Taxonomy path is wrong in file [" + files.get(0).getParent() + ":" + 
							key + "]: " + taxonomy + " (it's different from '" + genome.getTaxonomy() + "')");
				genome.withTaxonomy(taxonomy);
			}
		}
		// And all plasmids now
		for (String key : contigToOrgName.keySet()) {
			Boolean isPlasmid = contigToPlasmid.get(key);
			if (isPlasmid != null && !isPlasmid)
				continue;
			if (genome.getScientificName() == null)
				genome.setScientificName(contigToOrgName.get(key));
			String taxonomy = contigToTaxonomy.get(key);
			if (genome.getTaxonomy() == null && taxonomy != null)
				genome.withTaxonomy(taxonomy);
		}
		if (contigMap.size() == 0) {
			throw new Exception("GBK-file has no DNA-sequence");
		}
		String contigId = id + ".contigset";
		List<Long> contigLengths = new ArrayList<Long>();
		long dnaLen = 0;
		for (Contig contig : contigMap.values()) {
			if (contig.getSequence() == null || contig.getSequence().length() == 0) {
				throw new Exception("Contig " + contig.getId() + " has no DNA-sequence");
			}
			contig.withLength((long)contig.getSequence().length());
			contigLengths.add(contig.getLength());
			dnaLen += contig.getLength();
		}
		ContigSet contigSet = new ContigSet().withContigs(new ArrayList<Contig>(contigMap.values()))
				.withId(id).withMd5("md5").withName(id)
				.withSource("User uploaded data").withSourceId("USER").withType("Organism");
		wc.saveObjects(token, new SaveObjectsParams().withWorkspace(ws)
				.withObjects(Arrays.asList(new ObjectSaveData().withName(contigId)
						.withType("KBaseGenomes.ContigSet").withData(new UObject(contigSet)))));
		String ctgRef = ws + "/" + contigId;
		genome.withContigIds(new ArrayList<String>(contigMap.keySet())).withContigLengths(contigLengths)
				.withDnaSize(dnaLen).withContigsetRef(ctgRef).withFeatures(features)
				.withGcContent(calculateGcContent(contigSet));
		Map<String, String> meta = new LinkedHashMap<String, String>();
		meta.put("Scientific name", genome.getScientificName());
		wc.saveObjects(token, new SaveObjectsParams().withWorkspace(ws)
				.withObjects(Arrays.asList(new ObjectSaveData().withName(id).withMeta(meta)
						.withType("KBaseGenomes.Genome").withData(new UObject(genome)))));
	}

	public static double calculateGcContent(ContigSet contigs) {
		int at = 0;
		int gc = 0;
		for (Contig contig : contigs.getContigs()) {
			String seq = contig.getSequence();
			for (int i = 0; i < seq.length(); i++) {
				char ch = seq.charAt(i);
				if (ch == 'g' || ch == 'G' || ch == 'c' || ch == 'C') {
					gc++;
				} else if (ch == 'a' || ch == 'A' || ch == 't' || ch == 'T') {
					at++;
				}
			}
		}
		return (0.0 + gc) / (at + gc);
	}
}
