package core.index.robusttree;

import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.Range;
import core.utils.SchemaUtils;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by qui on 7/4/15.
 */
public class TestRobustTreeHs extends TestCase {

    String testTree1 = "17 4\n" +
            "INT INT STRING INT\n" +
            "n 0 INT 10\n" +
            "n 1 INT 32\n" +
            "n 2 STRING hello\n" +
            "n 3 INT 100\n" +
            "b 1\n" +
            "b 2\n" +
            "n 1 INT 18\n" +
            "b 3\n" +
            "b 4\n" +
            "n 3 INT 11\n" +
            "b 5\n" +
            "b 6\n" +
            "n 2 STRING hola\n" +
            "b 7\n" +
            "b 8\n";

    @Override
    public void setUp() {
    }

    public void testBucketRanges() {
        RobustTreeHs index = new RobustTreeHs(1);
        index.unmarshall(testTree1.getBytes());
        Map<Integer, Range> ranges1 = index.getBucketRanges(1);
        System.out.println(ranges1);
        Range[] expected = new Range[]{
                new Range(SchemaUtils.TYPE.INT, null, 32),
                new Range(SchemaUtils.TYPE.INT, null, 32),
                new Range(SchemaUtils.TYPE.INT, null, 18),
                new Range(SchemaUtils.TYPE.INT, 18, 32),
                new Range(SchemaUtils.TYPE.INT, 32, null),
                new Range(SchemaUtils.TYPE.INT, 32, null)
        };
        for (int i = 1; i <= 6; i++) {
            assertEquals(ranges1.get(i), expected[i-1]);
        }
        for (int i = 7; i <= 8; i++) {
            assertTrue(!ranges1.containsKey(i));
        }
    }

    public void testBucketRangesFile() {
        FileSystem fs = HDFSUtils.getFSByHadoopHome((new ConfUtils(Settings.cartilageConf)).getHADOOP_HOME());
        byte[] indexBytes = HDFSUtils.readFile(fs, "/user/qui/orders/index");
        RobustTreeHs index = new RobustTreeHs(1);
        index.unmarshall(indexBytes);
        System.out.println(index.getBucketRanges(0));
        System.out.println(Arrays.toString(index.getAllocations()));
    }

    public void testGetAllocations() {
        RobustTreeHs index = new RobustTreeHs(1);
        index.unmarshall(testTree1.getBytes());
        double[] expectedAllocations = new double[]{2.0, 1.25, 1.5, 0.75};
        double[] allocations = index.getAllocations();
        assertEquals(expectedAllocations.length, allocations.length);
        for (int i = 0; i < expectedAllocations.length; i++) {
            assertEquals(expectedAllocations[i], allocations[i]);
        }
        System.out.println(Arrays.toString(allocations));
    }

    public void testGetAllocationsEdge() {
        String indexString = "17 4\n" +
                "INT INT STRING INT\n" +
                "n 0 INT 10\n" +
                "n 1 INT 32\n" +
                "n 2 STRING hello\n" +
                "n 3 INT 100\n" +
                "b 1\n" +
                "b 2\n" +
                "b 3\n" +
                "b 4\n" +
                "b 5\n";
        RobustTreeHs index = new RobustTreeHs(1);
        index.unmarshall(indexString.getBytes());
        double[] expectedAllocations = new double[]{2.0, 1.0, 0.5, 0.25};
        double[] allocations = index.getAllocations();
        System.out.println(Arrays.toString(allocations));
        assertEquals(expectedAllocations.length, allocations.length);
        for (int i = 0; i < expectedAllocations.length; i++) {
            assertEquals(expectedAllocations[i], allocations[i]);
        }
    }
}
