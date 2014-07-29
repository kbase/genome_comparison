package us.kbase.genomecomparison;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Utils {
    public static int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public static int compare(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }
    
    public static long copy(InputStream from, OutputStream to) throws IOException {
    	byte[] buf = new byte[10000];
    	long total = 0;
    	while (true) {
    		int r = from.read(buf);
    		if (r == -1) {
    			break;
    		}
    		to.write(buf, 0, r);
    		total += r;
    	}
    	return total;
    }
}
