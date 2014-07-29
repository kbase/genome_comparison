package us.kbase.genomecomparison.gbk;

public class GbkParsingParams {
	private final boolean ignoreWrongFeatureLocation;
	
	public GbkParsingParams(boolean ignoreWrongFeatureLocation) {
		this.ignoreWrongFeatureLocation = ignoreWrongFeatureLocation;
	}
	
	public boolean isIgnoreWrongFeatureLocation() {
		return ignoreWrongFeatureLocation;
	}
}
