package us.kbase.genomecomparison.tree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import us.kbase.genomecomparison.FastaWriter;

public class TreePeparation {
	
	private static final Map<Integer, Integer> taxSubst = new HashMap<Integer, Integer>();
	
	static {
		taxSubst.put(269483, 482957);
	}
	
	public static void main(String[] args) throws Exception {
		File inputList = new File("/Users/rsutormin/Work/2014-03-11_species_tree/COGlist");
		File alnDir = new File("/Users/rsutormin/Work/2014-03-11_species_tree/speciestree/");
		File outDir = new File("data/tree");
		Map<Integer, String> taxMap = TaxTreeLoader.loadTaxNames();
		List<Integer> cogCodes = new ArrayList<Integer>();
		BufferedReader br = new BufferedReader(new FileReader(inputList));
		while (true) {
			String l = br.readLine();
			if (l == null)
				break;
			if (l.trim().length() == 0)
				continue;
			cogCodes.add(Integer.parseInt(l.trim()));
		}
		br.close();
		Map<Integer, Integer> wrongTax = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> commonIdSet = new TreeMap<Integer, Integer>();
		for (Integer cogCode : cogCodes) {
			System.out.println("COG " + cogCode);
			File alnFile = new File(alnDir, "COG" + cogCode + ".trim");
			Map<Integer, String> tax2seq = loadTrimmedAlignment(alnFile);
			int size = 0;
			for (int tax : tax2seq.keySet()) {
				int taxId = taxSubst.containsKey(tax) ? taxSubst.get(tax) : tax;
				if (!taxMap.containsKey(taxId)) {
					Integer count = wrongTax.get(taxId);
					wrongTax.put(taxId, count == null ? 1 : (count + 1));
				} else {
					Integer count = commonIdSet.get(taxId);
					commonIdSet.put(taxId, count == null ? 1 : (count + 1));
					size++;
				}
			}
			System.out.println("\tsize=" + size);
		}
		for (int taxId : wrongTax.keySet())
			System.err.println("Unknown tax id: " + taxId + " (" + wrongTax.get(taxId) + ")");
		Map<Integer, String> concat = new TreeMap<Integer, String>();
		for (Integer cogCode : cogCodes) {
			File alnFile = new File(alnDir, "COG" + cogCode + ".trim");
			Map<Integer, String> tax2seq = loadTrimmedAlignment(alnFile);
			Map<Integer, String> outAln = new TreeMap<Integer, String>();
			int alnLen = -1;
			for (int tax : tax2seq.keySet()) {
				int taxId = taxSubst.containsKey(tax) ? taxSubst.get(tax) : tax;
				if (!taxMap.containsKey(taxId))
					continue;
				//if (commonIdSet.get(taxId) == cogCodes.size())
				outAln.put(taxId, tax2seq.get(tax).toString());
				if (alnLen < 0)
					alnLen = tax2seq.get(tax).length();
			}
			for (int taxId : commonIdSet.keySet()) {
				String seq = outAln.get(taxId);
				if (seq == null) {
					char[] arr = new char[alnLen];
					Arrays.fill(arr, '-');
					seq = new String(arr);
				}
				String prev = concat.get(taxId);
				if (prev == null)
					prev = "";
				concat.put(taxId, prev + seq);
			}
			File outFile = new File(outDir, "COG" + cogCode + ".trim.faa.gz");
			GZIPOutputStream os = new GZIPOutputStream(new FileOutputStream(outFile));
			FastaWriter fw = new FastaWriter(new PrintWriter(new OutputStreamWriter(os)));
			for (int taxId : outAln.keySet())
				fw.write("" + taxId, outAln.get(taxId).toString());
			fw.close();
			System.out.println("COG " + cogCode + ", size=" + outAln.size());
		}
		FastaWriter fw = new FastaWriter(new File(outDir, "common.faa"));
		for (int taxId : concat.keySet())
			fw.write(convertNameToId(taxMap.get(taxId)) + "_" + taxId, concat.get(taxId));
		fw.close();
	}
	
	private static String convertNameToId(String name) {
		StringBuilder ret = new StringBuilder();
		for (int i = 0; i < name.length(); i++) {
			char ch = name.charAt(i);
			if (Character.isLetter(ch) || Character.isDigit(ch)) {
				ret.append(ch);
			} else {
				ret.append('_');
			}
		}
		return ret.toString();
	}
	
	private static Map<Integer, String> loadTrimmedAlignment(File f) throws IOException {
		Map<Integer, String> ret = new LinkedHashMap<Integer, String>();
		BufferedReader br = new BufferedReader(new FileReader(f));
		try {
			br.readLine();
			List<Integer> ids = new ArrayList<Integer>();
			int curPos = 0;
			boolean firstBlock = true;
			for (int line = 0; ; line++) {
				String l = br.readLine();
				if (l == null)
					break;
				if (l.trim().length() == 0) {
					curPos = 0;
					firstBlock = false;
					continue;
				}
				if (l.length() < 11)
					throw new IllegalStateException("Wrong line format [" + l + "] at " + line);
				String prefix = l.substring(0, 11).trim();
				int id = -1;
				if (curPos >= ids.size()) {
					if (!firstBlock)
						throw new IllegalStateException("Internal error");
					id = Integer.parseInt(prefix);
					ids.add(id);
				} else {
					if (firstBlock)
						throw new IllegalStateException("Internal error");
					if (prefix.length() > 0)
						throw new IllegalStateException("Wrong prefix format [" + prefix + "] at " + line);
					id = ids.get(curPos);
				}
				l = l.substring(11);
				String[] parts = l.split(Pattern.quote(" "));
				StringBuilder sb = new StringBuilder();
				for (String part : parts)
					sb.append(part);
				String seq = ret.get(id);
				if (seq == null) {
					if (!firstBlock)
						throw new IllegalStateException("Internal error");
					seq = "";
				}
				ret.put(id, seq + sb.toString());
				curPos++;
			}
			return ret;
		} finally {
			br.close();
		}
	}
}
