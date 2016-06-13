package core.common.index;


import core.adapt.Predicate;
import core.common.globals.TableInfo;
import core.common.key.ParsedTupleList;
import core.common.key.RawIndexKey;
import core.utils.Pair;
import core.utils.TypeUtils.TYPE;

import java.util.*;

/**
 * Created by ylu on 1/21/16.
 */


public class JoinRobustTree implements MDIndex {
    public static Random randGenerator = new Random();
    public TableInfo tableInfo;
    public int maxBuckets;
    public int numAttributes;
    public int joinAttributeDepth;
    public ParsedTupleList sample;
    public TYPE[] dimensionTypes;
    JRNode root;

    public JoinRobustTree(TableInfo tableInfo) {
        this.root = new JRNode();
        this.tableInfo = tableInfo;
    }

    public JoinRobustTree() {
    }

    /**
     * Return the dimension which has the maximum allocation unfulfilled
     */
    public static int getLeastAllocated(double[] allocations, boolean[] validDims) {

        boolean validFound = false;
        double minAlloc = 0;

        ArrayList<Integer> leastAllocated = new ArrayList<Integer>();

        for (int i = 0; i < allocations.length; i++) {
            if (validDims[i] == false) {
                continue;
            }
            if (validFound == false) {
                validFound = true;
                minAlloc = allocations[i];
                leastAllocated.add(i);
            } else {
                if (allocations[i] > minAlloc) {
                    minAlloc = allocations[i];
                    leastAllocated.clear();
                    leastAllocated.add(i);
                } else if (allocations[i] == minAlloc) {
                    leastAllocated.add(i);
                }
            }
        }

        if (leastAllocated.size() == 1) {
            return leastAllocated.get(0);
        } else {
            int r = randGenerator.nextInt(leastAllocated.size());
            return leastAllocated.get(r);
        }
    }

    public static void printNode(JRNode node) {
        if (node.bucket != null) {
            System.out.println("B " + node.bucket.getBucketId());
        } else {
            /*
            System.out.format("Node: %d %s { ", node.attribute,
                    node.value.toString());
            printNode(node.leftChild);
            System.out.print(" }{ ");
            printNode(node.rightChild);
            System.out.print(" }");
            */
            printNode(node.leftChild);
            printNode(node.rightChild);
        }
    }

    private static Map<Integer, BucketInfo> getBucketRangeSubtree(JRNode root,
                                                                  int attribute) {
        if (root.bucket != null) {
            Map<Integer, BucketInfo> bucketRanges = new HashMap<Integer, BucketInfo>();
            if (root.rangesByAttribute.get(attribute) != null) {
                bucketRanges.put(root.bucket.getBucketId(),
                        root.rangesByAttribute.get(attribute));
            }
            return bucketRanges;
        } else {
            Map<Integer, BucketInfo> bucketRanges = getBucketRangeSubtree(
                    root.leftChild, attribute);
            bucketRanges.putAll(getBucketRangeSubtree(root.rightChild,
                    attribute));
            return bucketRanges;
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

    public JRNode getRoot() {
        return root;
    }

    // Only used for testing
    public void setRoot(JRNode root) {
        this.root = root;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JoinRobustTree) {
            JoinRobustTree rhs = (JoinRobustTree) obj;
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
        System.out.println("method not implemented!");
    }

    @Override
    public void initProbe(int joinAttribute) {
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

        /**
         * Do a level-order traversal
         */
        LinkedList<Task> nodeQueue = new LinkedList<Task>();
        // Initialize root with attribute 0
        Task initialTask = new Task();
        initialTask.node = root;
        initialTask.sample = this.sample;
        initialTask.depth = 1;
        nodeQueue.add(initialTask);

        while (nodeQueue.size() > 0) {
            Task t = nodeQueue.pollFirst();


            if (t.depth > maxDepth) {
                Bucket b = new Bucket();
                b.setSample(t.sample);
                t.node.bucket = b;
                continue;
            }

            int dim = -1;
            Pair<ParsedTupleList, ParsedTupleList> halves = null;

            if (t.depth <= this.joinAttributeDepth) {
                dim = joinAttribute;
                allocations[dim] -= 2.0 / Math.pow(2, t.depth - 1);
            } else if (t.depth <= maxDepth) {

                boolean[] validDims = new boolean[numAttributes];
                Arrays.fill(validDims, true);

                for (int i = 0; i < numAttributes; i++) {
                    int testDim = getLeastAllocated(allocations, validDims);
                    halves = t.sample.sortAndSplit(testDim);
                    if (halves.first.size() > 0 && halves.second.size() > 0) {
                        dim = testDim;
                        allocations[dim] -= 2.0 / Math.pow(2, t.depth - 1);
                        break;
                    } else {
                        validDims[testDim] = false;
                        System.err.println("WARN: Skipping attribute " + testDim);
                    }
                }

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

                t.node.leftChild = new JRNode();
                t.node.leftChild.parent = t.node;
                Task tl = new Task();
                tl.node = t.node.leftChild;
                tl.depth = t.depth + 1;
                tl.sample = halves.first;
                nodeQueue.add(tl);

                t.node.rightChild = new JRNode();
                t.node.rightChild.parent = t.node;
                Task tr = new Task();
                tr.node = t.node.rightChild;
                tr.depth = t.depth + 1;
                tr.sample = halves.second;
                nodeQueue.add(tr);
            }
        }

        System.out.println("Final Allocations: " + Arrays.toString(allocations));
    }

    /**
     * Used in the 2nd phase of upfront to assign each tuple to the right
     */
    @Override
    public Object getBucketId(RawIndexKey key) {
        return root.getBucketId(key);
    }

    private void getAllBucketsHelper(JRNode node, ArrayList<Integer> ids) {
        if (node.bucket != null) {
            ids.add(node.bucket.getBucketId());
            return;
        }
        getAllBucketsHelper(node.leftChild, ids);
        getAllBucketsHelper(node.rightChild, ids);
    }

    public int[] getAllBuckets() {
        ArrayList<Integer> ids = new ArrayList<Integer>();
        getAllBucketsHelper(root, ids);
        int[] buckets = new int[ids.size()];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = ids.get(i);
        }
        return buckets;
    }

    /***************************************************
     * **************** RUNTIME METHODS *****************
     ***************************************************/

    public List<JRNode> getMatchingBuckets(Predicate[] predicates) {
        List<JRNode> results = root.search(predicates);
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
        robustTree += String.format("%d %d %d\n", this.maxBuckets,
                this.numAttributes, this.joinAttributeDepth);

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
        this.joinAttributeDepth = sc.nextInt();

        this.dimensionTypes = new TYPE[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            this.dimensionTypes[i] = TYPE.valueOf(sc.next());
        }

        this.root = new JRNode();
        this.root.parseNode(sc);
    }

    public void loadSample(TableInfo tableInfo, byte[] bytes) {
        this.sample = new ParsedTupleList(tableInfo.getTypeArray());
        this.sample.unmarshall(bytes, tableInfo.delimiter);
        this.dimensionTypes = this.sample.getTypes();
        this.numAttributes = this.dimensionTypes.length;
        this.initializeBucketSamplesAndCounts(this.root, this.sample, this.sample.size(), tableInfo.numTuples);
    }

    public void loadSample(ParsedTupleList sample) {
        this.sample = sample;
        this.dimensionTypes = this.sample.getTypes();
        this.numAttributes = this.dimensionTypes.length;
    }

    public void initializeBucketSamplesAndCounts(JRNode n,
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

    public Map<Integer, BucketInfo> getBucketRanges(int attribute) {
        return getBucketRangeSubtree(this.getRoot(), attribute);
    }

    private void getAllocations_helper(JRNode node, int depth, double[] allocArray) {
        if (node.bucket != null) {
            return;
        }
        allocArray[node.attribute] += 2.0 / Math.pow(2, depth);
        getAllocations_helper(node.leftChild, depth + 1, allocArray);
        getAllocations_helper(node.rightChild, depth + 1, allocArray);
    }

    public double[] getAllocations() {
        double[] allocArray = new double[numAttributes];
        Arrays.fill(allocArray, 0);
        getAllocations_helper(root, 0, allocArray);
        return allocArray;
    }

    private void clearFlags_helper(JRNode node) {
        if (node.bucket != null) {
            return;
        }
        node.updated = false;
        node.fullAccessed = false;
        clearFlags_helper(node.leftChild);
        clearFlags_helper(node.rightChild);
    }

    public void clearFlags() {
        clearFlags_helper(root);
    }

    private boolean isUpdated_helper(JRNode node) {
        if (node.bucket != null) {
            return node.updated;
        }
        if (node.updated) {
            return true;
        }
        if (isUpdated_helper(node.leftChild)) {
            return true;
        } else {
            return isUpdated_helper(node.rightChild);
        }
    }

    public boolean isUpdated() {
        return isUpdated_helper(root);
    }

    public class Task {
        JRNode node;
        int depth;
        ParsedTupleList sample;
    }
}
