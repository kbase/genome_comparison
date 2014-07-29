package us.kbase.genomecomparison.test;

import java.util.List;

import us.kbase.auth.AuthService;
import us.kbase.genomecomparison.ContigSetUploadServlet;

public class GbkImportTester {
	public static void main(String[] args) throws Exception {
		String user = "nardevuser1";
		String pwd = "*****";
		String token = AuthService.login(user, pwd).getTokenString();
		List<String> genomeNames = ContigSetUploadServlet.getNcbiGenomeNames();
		System.out.println(genomeNames);
		ContigSetUploadServlet.importNcbiGenome(genomeNames.get(0), "nardevuser1:home", "ncbi_genome.1", token);
	}
}
