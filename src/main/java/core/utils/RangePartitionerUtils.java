package core.utils;

import core.common.globals.TableInfo;
import core.common.key.ParsedTupleList;
import org.apache.hadoop.fs.FileSystem;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by ylu on 12/14/15.
 */
public class RangePartitionerUtils {

    public static ArrayList<Long> getKeys(ConfUtils cfg, TableInfo tableInfo, String path, int dimension) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        // read all the sample files and put them into the sample key set
        ParsedTupleList sample = new ParsedTupleList(tableInfo.getTypeArray());
        byte[] bytes = HDFSUtils.readFile(fs, path);
        sample.unmarshall(bytes, tableInfo.delimiter);

        ArrayList<Long> keys = new ArrayList<Long>();

        for (Object[] o : sample.getValues()) {
            if (o[dimension] instanceof Long) {
                keys.add((Long) o[dimension]);
            } else if (o[dimension] instanceof Integer) {
                int key = (Integer) o[dimension];
                keys.add((long) key);
            }
        }

        return keys;

    }

    public static String getCutPoints(ArrayList<Long> sampleKeys, int recordSize, long count) {

        long chunkSize = 32 * 1024 * 1024; // 32 MB
        long totalSize = count * recordSize;
        int n = (int) ((totalSize + chunkSize - 1) / chunkSize); // ceiling

        long[] keys = new long[sampleKeys.size()];
        int i = 0;
        for (long lw : sampleKeys) {
            keys[i++] = lw;
        }
        Arrays.sort(keys);

        int m = keys.length;
        long[] cutpoints = new long[n - 1];

        int small = n - m % n;
        int large = m % n;

        int pi = 0, pj = 0;
        for (int j = 0; j < small; j++) {
            pj += m / n;
            if (pi < cutpoints.length) {
                cutpoints[pi++] = keys[pj];
            }

        }
        for (int j = 0; j < large; j++) {
            pj += m / n + 1;
            if (pi < cutpoints.length) {
                cutpoints[pi++] = keys[pj];
            }
        }

        String strCutPoints = getStringCutPoints(cutpoints);
        return strCutPoints;
    }

    public static long[] getLongCutPoints(String cutpoints) {
        if (cutpoints.equals("NULL")) {
            return null;
        }
        if (cutpoints.length() == 0) {
            return new long[0];
        }
        String[] values = cutpoints.split(",");
        long[] ranges = new long[values.length];
        for (int i = 0; i < ranges.length; i++) {
            ranges[i] = Long.parseLong(values[i]);
        }
        return ranges;
    }

    public static String getStringCutPoints(long[] cutpoints) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cutpoints.length; i++) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(cutpoints[i]);
        }
        return sb.toString();
    }

    public static int[] getSplits(long[] cutpoints) {
        int[] splits = new int[cutpoints.length + 1];
        for (int i = 0; i < splits.length; i++) {
            splits[i] = i;
        }
        return splits;
    }
}
