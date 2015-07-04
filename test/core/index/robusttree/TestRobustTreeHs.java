package core.index.robusttree;

import junit.framework.TestCase;

import java.util.Arrays;

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
        index.printBucketRanges(1);
        // TODO: make bucket ranges returned more meaningful
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
