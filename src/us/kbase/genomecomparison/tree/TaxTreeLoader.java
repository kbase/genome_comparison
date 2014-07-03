package us.kbase.genomecomparison.tree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxTreeLoader {
	private static final String workDir = "/Users/rsutormin/Work/2013-12-14_ncbi_tax";
	private static final String nodesFileName = "nodes.dmp";
	private static final String namesFileName = "names.dmp";
	private static final Pattern div = Pattern.compile(Pattern.quote("\t|\t")); 
	
	public static void main(String[] args) throws IOException {
		loadTaxTreeFromFiles();
	}
	
	public static Map<Integer, String> loadTaxNames() throws IOException {
		Map<Integer, String> nodeMap = new HashMap<Integer, String>();
		BufferedReader br = new BufferedReader(new FileReader(new File(workDir, namesFileName)));
		try {
			while (true) {
				String l = br.readLine();
				if (l == null)
					break;
				if (l.trim().length() == 0)
					continue;
				if (l.endsWith("\t|"))
					l = l.substring(0, l.length() - 2);
				String[] parts = div.split(l);
				if (parts.length != 4)
					throw new IllegalStateException("Wrong line format: [" + l + "]");
				if (!parts[3].equals("scientific name"))
					continue;
				int nodeId = Integer.parseInt(parts[0]);
				nodeMap.put(nodeId, parts[1]);
			}
			return nodeMap;
		} finally {
			br.close();
		}
	}
	
	public static Map<String, Object> loadTaxTreeFromFiles() throws IOException {
		Map<Integer, TreeNode> nodeMap = new HashMap<Integer, TreeNode>();
		BufferedReader br = new BufferedReader(new FileReader(new File(workDir, namesFileName)));
		while (true) {
			String l = br.readLine();
			if (l == null)
				break;
			if (l.trim().length() == 0)
				continue;
			if (l.endsWith("\t|"))
				l = l.substring(0, l.length() - 2);
			String[] parts = div.split(l);
			if (parts.length != 4)
				throw new IllegalStateException("Wrong line format: [" + l + "]");
			if (!parts[3].equals("scientific name"))
				continue;
			int nodeId = Integer.parseInt(parts[0]);
			nodeMap.put(nodeId, new TreeNode(nodeId, parts[1]));
		}
		br.close();
		System.out.println("Nodes: " + nodeMap.size());
		int rootId = -1;
		Set<String> hidden = new HashSet<String>();
		br = new BufferedReader(new FileReader(new File(workDir, nodesFileName)));
		while (true) {
			String l = br.readLine();
			if (l == null)
				break;
			if (l.trim().length() == 0)
				continue;
			if (l.endsWith("\t|"))
				l = l.substring(0, l.length() - 2);
			String[] parts = div.split(l);
			if (parts.length < 12)
				throw new IllegalStateException("Wrong line format (" + parts.length + " fields): [" + l + "]");
			int nodeId = Integer.parseInt(parts[0]);
			int parentId = Integer.parseInt(parts[1]);
			if (nodeId == parentId) {
				rootId = nodeId;
				continue;
			}
			if (parts[10].equals("1")) {
				hidden.add("" + nodeId);
			}
			TreeNode parent = nodeMap.get(parentId);
			if (parent == null)
				throw new IllegalStateException("No node for parent id=" + parentId);
			TreeNode node = nodeMap.get(nodeId);
			if (node == null)
				throw new IllegalStateException("No node for id=" + nodeId);
			if (parent.children == null)
				parent.children = new ArrayList<TreeNode>();
			parent.children.add(node);
		}
		br.close();
		System.out.println("Hidden in genbank: " + hidden.size());
		TreeNode root = nodeMap.get(rootId);
		removeHidden(null, root, hidden);
		System.out.println("Saved nodes: " + countNodes(root));
		Map<String, Object> ret = transform(root);
		File out = new File(workDir, "out.json");
		new ObjectMapper().writeValue(out, ret);
		return ret;
	}
	
	private static Map<String, Object> transform(TreeNode node) {
		Map<String, Object> ret = new LinkedHashMap<String, Object>();
		ret.put("title", node.title);
		ret.put("tax", node.tax);
		if (node.children != null && node.children.size() > 0) {
			List<Object> children = new ArrayList<Object>();
			for (TreeNode ch : node.children)
				children.add(transform(ch));
			ret.put("children", children);
		}
		return ret;
	}
	
	private static void removeHidden(TreeNode parent, TreeNode node, Set<String> hiddenNodes) {
		TreeNode futPar = node;
		if (hiddenNodes.contains("" + node.tax)) {
			parent.children.remove(node);
			if (node.children != null)
				for (TreeNode ch : node.children)
					parent.children.add(ch);
			futPar = parent;
		}
		if (node.children != null) {
			List<TreeNode> children = new ArrayList<TreeNode>(node.children);
			for (TreeNode ch : children)
				removeHidden(futPar, ch, hiddenNodes);
		}
	}
	
	private static int countNodes(TreeNode node) {
		int ret = 1;
		if (node.children != null) {
			for (TreeNode ch : node.children)
				ret += countNodes(ch);
		}
		return ret;
	}

	private static class TreeNode {
        public String title;
        public String tax;
        public List<TreeNode> children = new ArrayList<TreeNode>();

        TreeNode(int taxId, String name) {
            this.tax = "" + taxId;
        	this.title = name;
        }
    }
}
