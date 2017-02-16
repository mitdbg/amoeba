package core.common.index;

import core.adapt.Predicate;
import core.common.globals.TableInfo;
import core.common.key.ParsedTupleList;
import core.common.key.RawIndexKey;
import core.utils.Pair;
import core.utils.TypeUtils.TYPE;

import java.util.*;

public class RobustTree implements MDIndex {
    public transient static Random randGenerator = new Random();
    // TODO: Add capacity
    transient static List<Integer> leastAllocated = new ArrayList<>(50);
    /**
     * Return the dimension which has the maximum allocation unfulfilled
     */
    transient static int countCalled = -1;
    transient public ParsedTupleList sample;
    transient public TableInfo tableInfo;
    public int maxBuckets;
    public int numAttributes;
    public TYPE[] dimensionTypes;
    public RNode root;

    public RobustTree(TableInfo tableInfo) {
        this.root = new RNode();
        this.tableInfo = tableInfo;
    }

    public RobustTree() {
    }

    public static double nthroot(int n, double A) {
        return nthroot(n, A, .001);
    }

    public static double nthroot(int n, double A, double p) {
        if (A < 0) {
            System.err.println("A < 0");// we handle only real positive numbers
            return -1;
        } else if (A == 0) {
            return 0;
        }

        double x_prev = A;
        double x = A / n; // starting "guessed" value...
        while (Math.abs(x - x_prev) > p) {
            x_prev = x;
            x = ((n - 1.0) * x + A / Math.pow(x, n - 1.0)) / n;
        }

        return x;
    }

    public static void printNode(RNode node) {
        if (node.bucket != null) {
            System.out.format("B " + node.bucket.bucketId);
        } else {
            System.out.format("Node: %d %s { ", node.attribute,
                    node.value.toString());
            printNode(node.leftChild);
            System.out.print(" }{ ");
            printNode(node.rightChild);
            System.out.print(" }");
        }
    }

    public int getMaxBuckets() {
        return maxBuckets;
    }

    public void setMaxBuckets(int maxBuckets) {
        this.maxBuckets = maxBuckets;
    }

    @Override
    public MDIndex clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    public RNode getRoot() {
        return root;
    }

    // Only used for testing
    public void setRoot(RNode root) {
        this.root = root;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RobustTree) {
            RobustTree rhs = (RobustTree) obj;
            boolean allGood = true;
            allGood &= rhs.numAttributes == this.numAttributes;
            allGood &= rhs.maxBuckets == this.maxBuckets;
            allGood &= rhs.dimensionTypes.length == this.dimensionTypes.length;
            if (!allGood)
                return false;

            for (int i = 0; i < this.dimensionTypes.length; i++) {
                allGood &= this.dimensionTypes[i] == rhs.dimensionTypes[i];
            }
            if (!allGood)
                return false;

            allGood = this.root == rhs.root;
            return allGood;
        }
        return false;
    }

    /**
     * Created the tree based on the histograms
     */
    @Override
    public void initProbe() {
        System.out.println(this.sample.size() + " keys inserted");

        // Computes log(this.maxBuckets)
        int maxDepth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets);
        double allocationPerAttribute = RobustTree.nthroot(
                this.numAttributes, this.maxBuckets);
        System.out.println("Max allocation: " + allocationPerAttribute);

        double[] allocations = new double[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            allocations[i] = allocationPerAttribute;
        }

        // Custom
//        double[] allocations = new double[this.numAttributes];
//        for (int i = 0; i < this.numAttributes; i++)
//            allocations[i] = -1;
//
//        allocations[tableInfo.schema.getAttributeId("o_orderdate")] = 12;
//        allocations[tableInfo.schema.getAttributeId("l_quantity")] = 6;
//        allocations[tableInfo.schema.getAttributeId("c_mktsegment")] = 4;
//        allocations[tableInfo.schema.getAttributeId("c_region")] = 4;

        /**
         * Do a level-order traversal
         */
        LinkedList<Task> nodeQueue = new LinkedList<Task>();
        // Initialize root with attribute 0
        Task initialTask = new Task();
        initialTask.node = root;
        initialTask.sample = this.sample;
        initialTask.depth = 0;
        nodeQueue.add(initialTask);

        while (nodeQueue.size() > 0) {
            Task t = nodeQueue.pollFirst();
            if (t.depth < maxDepth) {
                int dim = -1;
                int round = 0;
                Pair<ParsedTupleList, ParsedTupleList> halves = null;

                while (dim == -1 && round < allocations.length) {
                    int testDim = getLeastAllocated(allocations);
                    allocations[testDim] -= 2.0 / Math.pow(2, t.depth);

                    // TODO: For low cardinality values, it might be better to
                    // choose some set of values on each side.
                    // TPCH attribute 9 for example has only two distinct values
                    // TODO: This might repeatedly use the same attribute
                    halves = t.sample.sortAndSplit(testDim);
                    if (halves.first.size() > 0 && halves.second.size() > 0) {
                        dim = testDim;
                    } else {
                        System.err.println("WARN: Skipping attribute "
                                + testDim);
                    }

                    round++;
                }

                if (dim == -1) {
                    System.err.println("ERR: No attribute to partition on");
                    Bucket b = new Bucket();
                    b.setSample(sample);
                    t.node.bucket = b;
                } else {
                    t.node.attribute = dim;
                    t.node.type = this.dimensionTypes[dim];
                    t.node.value = halves.first.getLast(dim); // Need to traverse up for range.

                    t.node.leftChild = new RNode();
                    t.node.leftChild.parent = t.node;
                    Task tl = new Task();
                    tl.node = t.node.leftChild;
                    tl.depth = t.depth + 1;
                    tl.sample = halves.first;
                    nodeQueue.add(tl);

                    t.node.rightChild = new RNode();
                    t.node.rightChild.parent = t.node;
                    Task tr = new Task();
                    tr.node = t.node.rightChild;
                    tr.depth = t.depth + 1;
                    tr.sample = halves.second;
                    nodeQueue.add(tr);
                }
            } else {
                Bucket b = new Bucket();
                b.setSample(sample);
                t.node.bucket = b;
            }
        }

        System.out
                .println("Final Allocations: " + Arrays.toString(allocations));
    }

    @Override
    public void initProbe(int joinAttribute) {
        System.out.println("method not implemented!");
    }

    public int getLeastAllocated(double[] allocations) {
        int numAttributes = allocations.length;
//        if (countCalled < 2) {
//            countCalled++;
//            if (countCalled == 0) {
//                return tableInfo.schema.getAttributeId("o_orderdate"); // o_orderdate
//            } else if (countCalled == 1) {
//                return tableInfo.schema.getAttributeId("l_shipmode"); // l_shipmode
//            } else if (countCalled == 2) {
//                return tableInfo.schema.getAttributeId("l_shipmode"); // l_shipmode
//            }
//            // Could be useful to add quantity
//        }

        if (countCalled < 0) {
            countCalled++;
            if (countCalled == 0) {
                return tableInfo.schema.getAttributeId("sf_uploadtime"); // o_orderdate
            }
            // Could be useful to add quantity
        }

        leastAllocated.clear();
        leastAllocated.add(0);

        double alloc = allocations[0];
        for (int i = 1; i < numAttributes; i++) {
            if (allocations[i] > alloc) {
                alloc = allocations[i];
                leastAllocated.clear();
                leastAllocated.add(i);
            } else if (allocations[i] == alloc) {
                leastAllocated.add(i);
            }
        }

        if (leastAllocated.size() == 1) {
            return leastAllocated.get(0);
        } else {
            int r = randGenerator.nextInt(leastAllocated.size());
            return leastAllocated.get(r);
        }
    }

    /**
     * Used in the 2nd phase of upfront to assign each tuple to the right
     */
    @Override
    public Integer getBucketId(RawIndexKey key) {
        return root.getBucketId(key);
    }

    public int[] getAllBucketIds() {
       return root.getAllBucketIds();
    }

    /***************************************************
     * **************** RUNTIME METHODS *****************
     ***************************************************/

    public List<RNode> getMatchingBuckets(Predicate[] predicates) {
        List<RNode> results = root.search(predicates);
        return results;
    }

    /**
     * Serializes the index to string Very brittle - Consider rewriting
     */
    @Override
    public byte[] marshall() {
        // JVM optimizes shit so no need to use string builder / buffer
        // Format:
        // maxBuckets, numAttributes
        // types
        // nodes in pre-order

        String robustTree = "";
        robustTree += String.format("%d %d\n", this.maxBuckets,
                this.numAttributes);

        String types = "";
        for (int i = 0; i < this.numAttributes; i++) {
            types += this.dimensionTypes[i].toString() + " ";
        }
        types += "\n";
        robustTree += types;

        robustTree += this.root.marshall();

        return robustTree.getBytes();
    }

    @Override
    public void unmarshall(byte[] bytes) {
        String tree = new String(bytes);
        Scanner sc = new Scanner(tree);
        this.maxBuckets = sc.nextInt();
        this.numAttributes = sc.nextInt();

        this.dimensionTypes = new TYPE[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            this.dimensionTypes[i] = TYPE.valueOf(sc.next());
        }

        // Reset the maxBucketId.
        Bucket.maxBucketId = 0;

        this.root = new RNode();
        this.root.parseNode(sc);
    }

    public void loadSample(TableInfo tableInfo, byte[] bytes) {
        this.sample = new ParsedTupleList(tableInfo.getTypeArray());
        this.sample.unmarshall(bytes, tableInfo.delimiter);
        this.initializeBucketSamplesAndCounts(this.root, this.sample, this.sample.size(), tableInfo.numTuples);
    }

    public void loadSample(ParsedTupleList sample) {
        this.sample = sample;
        this.dimensionTypes = this.sample.getTypes();
        this.numAttributes = this.dimensionTypes.length;
    }

    public void initializeBucketSamplesAndCounts(RNode n,
                                                 ParsedTupleList sample, final double totalSamples,
                                                 final double totalTuples) {
        if (n.bucket != null) {
            long sampleSize = sample.size();
            double numTuples = (sampleSize * totalTuples) / totalSamples;
            n.bucket.setSample(sample);
            n.bucket.setEstimatedNumTuples(numTuples);
        } else {
            // By sorting we avoid memory allocation
            // Will most probably be faster
            sample.sort(n.attribute);
            Pair<ParsedTupleList, ParsedTupleList> halves = sample
                    .splitAt(n.attribute, n.value);
            initializeBucketSamplesAndCounts(n.leftChild, halves.first,
                    totalSamples, totalTuples);
            initializeBucketSamplesAndCounts(n.rightChild, halves.second,
                    totalSamples, totalTuples);
        }
    }

    /**
     * Prints the tree created. Call only after initProbe is done.
     */
    public void printTree() {
        printNode(root);
    }

    public double[] getAllocations() {
        List<RNode> queue = new LinkedList<RNode>();
        Map<Integer, Double> allocs = new HashMap<Integer, Double>();
        queue.add(this.getRoot());

        int nodeNum = 0;
        RNode filler = new RNode();
        filler.attribute = -1;
        int lastNode = 0;
        while (queue.size() > 0) {
            nodeNum++;
            RNode node = queue.remove(0);
            if (node.bucket != null || node.attribute == -1) {
                if (nodeNum > lastNode * 2) {
                    break;
                }
                queue.add(filler);
                queue.add(filler);
                continue;
            }

            lastNode = nodeNum;
            if (!(allocs.containsKey(node.attribute))) {
                allocs.put(node.attribute, 0.0);
            }

            double addedAlloc = Math.pow(2, -1 * Math.floor(Math.log(nodeNum) / Math.log(2)) + 1);
            allocs.put(node.attribute, allocs.get(node.attribute) + addedAlloc);
            queue.add(node.leftChild);
            queue.add(node.rightChild);
        }

        double[] allocArray = new double[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
            if (!(allocs.containsKey(i))) {
                allocArray[i] = 0;
            } else {
                allocArray[i] = allocs.get(i);
            }
        }
        System.out.println(nodeNum);
        return allocArray;
    }

    public class Task {
        public RNode node;
        public int depth;
        public ParsedTupleList sample;
    }
}
