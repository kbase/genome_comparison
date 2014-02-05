package us.kbase.genomecomparison.gbk;

import java.util.List;

/**
 * Results of parsing will be pushed back through implementation of this class.
 * @author rsutormin
 */
public interface GbkCallback {
	public void addHeader(String contigName, String headerType, String value, List<GbkSubheader> items) throws Exception;
    public void addFeature(String contigName, String featureType, int strand, int start, int stop, List<GbkLocation> locations, List<GbkQualifier> props) throws Exception;
    public void setGenome(String genomeName, int taxId) throws Exception;
    public void addSeqPart(String contigName, int seqPartIndex, String seqPart, int commonLen) throws Exception;
}
