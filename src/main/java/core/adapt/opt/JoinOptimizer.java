package core.adapt.opt;


import java.io.IOException;
import java.util.*;

import core.adapt.JoinQuery;
import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PartitionIterator;
import core.adapt.spark.join.SparkJoinQueryConf;
import core.common.globals.TableInfo;
import core.common.index.JRNode;
import core.common.index.JoinRobustTree;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.adapt.Predicate;
import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.iterator.RepartitionIterator;


import core.common.index.MDIndex.Bucket;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.Pair;
import core.utils.TypeUtils;
import core.utils.TypeUtils.TYPE;
/**
 * Created by ylu on 1/21/16.
 */


/**
 * Optimizer creates the execution plans for the queries.
 * It uses the incoming query predicates as hints for what should be added
 * into the partitioning tree. If we find a plan which has benefit >
 * cost, we do the repartitioning. Else we just do a scan.
 *
 * @author yilu
 */
public class JoinOptimizer {
    private static final double WRITE_MULTIPLIER = 1.5;

    private JoinRobustTree rt;

    // Properties extracted from ConfUtils
    private String workingDir;
    private String hadoopHome;
    private short fileReplicationFactor;

    private TableInfo tableInfo;

    private List<JoinQuery> queryWindow = new ArrayList<JoinQuery>();

    public JoinOptimizer(SparkJoinQueryConf cfg) {
        // Working Directory for the Optimizer.
        // Each table is a folder under this directory.
        this.workingDir = cfg.getWorkingDir();
        this.hadoopHome = cfg.getHadoopHome();
        this.fileReplicationFactor = cfg.getHDFSReplicationFactor();
    }

    public JoinOptimizer(ConfUtils cfg) {
        this.workingDir = cfg.getHDFS_WORKING_DIR();
        this.hadoopHome = cfg.getHADOOP_HOME();
        this.fileReplicationFactor = cfg.getHDFS_REPLICATION_FACTOR();
    }

    public void loadIndex(TableInfo tableInfo) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        String tableDir = this.workingDir + "/" + tableInfo.tableName;
        String pathToIndex = tableDir + "/index";
        String pathToSample = tableDir + "/sample";

        this.tableInfo = tableInfo;

        System.out.println("Load index: " + pathToIndex);

        byte[] indexBytes = HDFSUtils.readFile(fs, pathToIndex);
        this.rt = new JoinRobustTree(tableInfo);
        this.rt.unmarshall(indexBytes);

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
        this.rt.loadSample(tableInfo, sampleBytes);
    }

    public void printTree() {
        rt.printTree();
    }

    public JoinRobustTree getIndex() {
        return rt;
    }

    public int[] getBidFromRNodes(List<JRNode> nodes) {
        int[] bids = new int[nodes.size()];
        Iterator<JRNode> it = nodes.iterator();
        for (int i = 0; i < bids.length; i++) {
            bids[i] = it.next().bucket.getBucketId();
        }
        return bids;
    }

    public PartitionSplit[] buildAccessPlan(JoinQuery fq) {
        List<JRNode> nodes = this.rt.getRoot().search(fq.getPredicates());
        PartitionIterator pi = new PostFilterIterator(fq.castToQuery());
        int[] bids = this.getBidFromRNodes(nodes);

        PartitionSplit psplit = new PartitionSplit(bids, pi);
        PartitionSplit[] ps = new PartitionSplit[1];
        ps[0] = psplit;
        return ps;
    }


    public int getTotalSampleSize(JRNode node) {
        if (node.bucket != null) {
            int size = node.bucket.getSample().size();
            return size;
        }
        return getTotalSampleSize(node.leftChild) + getTotalSampleSize(node.rightChild);
    }

    public int getJoinAttribute(JRNode node, int depth) {
        if (node.bucket != null) {
            return -1; // in case joinAttributesDepth >= tree's height
        }

        int attributeOnThisNode = node.attribute;
        if (depth == this.rt.joinAttributeDepth) {
            return attributeOnThisNode;
        } else {
            int attributeLeftChild = getJoinAttribute(node.leftChild, depth + 1);
            int attributeRightChild = getJoinAttribute(node.rightChild, depth + 1);

            if (attributeLeftChild != attributeOnThisNode || attributeRightChild != attributeOnThisNode) {
                return -1;
            } else {
                return attributeOnThisNode;
            }
        }
    }


    private int getNumJoinAttributes(int joinAttribute) {
        int num = 0;
        for (int i = 0; i < queryWindow.size(); i++) {
            if (queryWindow.get(i).getJoinAttribute() == joinAttribute) {
                num++;
            }
        }
        return num;
    }

    private void getAllSamples(JRNode node, ParsedTupleList collector) {
        if (node.bucket != null) {
            collector.addValues(node.bucket.getSample().getValues());
        } else {
            getAllSamples(node.leftChild, collector);
            getAllSamples(node.rightChild, collector);
        }
    }

    private void setJoinAttribute(JoinQuery q, JRNode node, ParsedTupleList sample, double[] allocations, int depth) {
        // depth <= joinAttributesDepth, set joinAttribute
        // depth > joinAttributesDepth, use old attributes
        // update estimation

        int joinAttribute = q.getJoinAttribute();

        if (node.bucket != null) {
            node.bucket.setEstimatedNumTuples(1.0 * sample.size() / rt.sample.size() * tableInfo.numTuples);
            node.bucket.setSample(sample);
            node.updated = true;
            node.fullAccessed = true;
            return;
        }

        if (depth <= this.rt.joinAttributeDepth) {
            Pair<ParsedTupleList, ParsedTupleList> halves = sample.sortAndSplit(joinAttribute);

            if (halves.first.size() > 0 && halves.second.size() > 0) {

                node.attribute = joinAttribute;
                node.type = tableInfo.getTypeArray()[joinAttribute]; // should be LONG
                node.value = halves.first.getLast(joinAttribute); // should equals to median
                node.fullAccessed = true;

                allocations[joinAttribute] -= 2.0 / Math.pow(2, depth - 1);

                setJoinAttribute(q, node.leftChild, halves.first, allocations, depth + 1);
                setJoinAttribute(q, node.rightChild, halves.second, allocations, depth + 1);

            } else {
                //System.err.println("ERR: No attribute to partition on");
                node.bucket = new Bucket();
                node.bucket.setEstimatedNumTuples(1.0 * sample.size() / rt.sample.size() * tableInfo.numTuples);
                node.bucket.setSample(sample);
                // new node
                node.updated = false;
                node.fullAccessed = false;

                markUpdatedBucket(node.leftChild);
                markUpdatedBucket(node.rightChild);

                node.leftChild = null;
                node.rightChild = null;
            }

        } else {
            node.fullAccessed = true;

            // use attributes from the query

            Pair<ParsedTupleList, ParsedTupleList> halves = null;

            int numAttributes = this.rt.numAttributes;
            boolean[] validDims = new boolean[numAttributes];
            Arrays.fill(validDims, false);

            Predicate[] ps = q.getPredicates();

            int numPredicates = 0;

            for (int i = 0; i < ps.length; i++) {
                if (validDims[ps[i].attribute] == false) {
                    validDims[ps[i].attribute] = true;
                    numPredicates++;
                }

            }

            int dim = -1;

            for (int i = 0; i < numPredicates; i++) {
                int testDim = JoinRobustTree.getLeastAllocated(allocations, validDims);
                halves = sample.sortAndSplit(testDim);
                if (halves.first.size() > 0 && halves.second.size() > 0) {
                    dim = testDim;
                    allocations[dim] -= 2.0 / Math.pow(2, depth - 1);
                    break;
                } else {
                    validDims[testDim] = false;
                    //System.err.println("WARN: Skipping attribute " + testDim);
                }
            }

            if (dim == -1) {
                Arrays.fill(validDims, true);
                for (int i = 0; i < numAttributes; i++) {
                    int testDim = JoinRobustTree.getLeastAllocated(allocations, validDims);
                    halves = sample.sortAndSplit(testDim);
                    if (halves.first.size() > 0 && halves.second.size() > 0) {
                        dim = testDim;
                        allocations[dim] -= 2.0 / Math.pow(2, depth - 1);
                        break;
                    } else {
                        validDims[testDim] = false;
                        //System.err.println("WARN: Skipping attribute " + testDim);
                    }
                }
            }

            if (dim == -1) {
                //System.err.println("ERR: No attribute to partition on");
                node.bucket = new Bucket();
                node.bucket.setEstimatedNumTuples(1.0 * sample.size() / rt.sample.size() * tableInfo.numTuples);
                node.bucket.setSample(sample);
                // new node
                node.updated = false;
                node.fullAccessed = false;
                markUpdatedBucket(node.leftChild);
                markUpdatedBucket(node.rightChild);
                node.leftChild = null;
                node.rightChild = null;
            } else {
                node.attribute = dim;
                node.type = this.tableInfo.getTypeArray()[dim];
                node.value = halves.first.getLast(dim); // Need to traverse up for range.

                setJoinAttribute(q, node.leftChild, halves.first, allocations, depth + 1);
                setJoinAttribute(q, node.rightChild, halves.second, allocations, depth + 1);
            }
        }
    }

    private void setJoinAttribute(JoinQuery q) {

        // grab all samples
        ParsedTupleList collector = new ParsedTupleList(tableInfo.getTypeArray());
        getAllSamples(rt.getRoot(), collector);

        // Computes log(this.maxBuckets)
        int maxDepth = 31 - Integer.numberOfLeadingZeros(this.rt.maxBuckets);
        double allocationPerAttribute = Math.pow(this.rt.maxBuckets, 1.0 / this.rt.numAttributes);

        double[] allocations = new double[this.rt.numAttributes];
        for (int i = 0; i < this.rt.numAttributes; i++) {
            allocations[i] = allocationPerAttribute;
        }

        // set join attributes
        setJoinAttribute(q, rt.getRoot(), collector, allocations, 1);
    }

    public void markAccessedBucket(JRNode changed, JoinQuery q) {
        List<JRNode> nodesAccessed = changed.search(q.getPredicates());

        for (JRNode node : nodesAccessed) {
            node.fullAccessed = true;
        }
    }

    public void markUpdatedBucket(JRNode node){
        if(node.bucket != null){
            node.updated = true;
            return;
        }
        markUpdatedBucket(node.leftChild);
        markUpdatedBucket(node.rightChild);
    }


    public PartitionSplit[] buildPlan(JoinQuery q) {

        this.queryWindow.add(q);

        Predicate[] ps = q.getPredicates();
        LinkedList<Predicate> choices = new LinkedList<Predicate>();

        // Initialize the set of choices for predicates.
        for (int i = 0; i < ps.length; i++) {
            choices.add(ps[i]);
        }

        int curJoinAttribute = getJoinAttribute(rt.getRoot(), 1);
        int numJoinAttributes = getNumJoinAttributes(q.getJoinAttribute());

        // the current join attribute is different and the number of queries which have the same joinAttributes is large

        JoinRobustTree.randGenerator.setSeed(0);

        boolean updated = false;

        byte[] oldJoinRobustTree = rt.marshall();

        List<JRNode> nodes = rt.getRoot().search(q.getPredicates());
        List<JRNode> allNodes = rt.getRoot().search(new Predicate[]{}); // get all nodes;

        boolean fullRepartition = false;

        if (q.getForceRepartition()) {
            System.out.println("Data is going to be fully repartitioned!");
            fullRepartition = true;
            double numAccessed = getNumTuplesAccessed(rt.getRoot(), q);
            setJoinAttribute(q);

            populateBucketEstimates(rt.getRoot());
            updated = true;
            System.out.println("Accessed tuple counts: " + numAccessed);

        } else {
            double totalAccessedOld = getNumTuplesAccessed(rt.getRoot());
            double AccessedOld = getNumTuplesAccessed(rt.getRoot(), q);
            adjustJoinRobustTree(choices, q);
            populateBucketEstimates(rt.getRoot());
            double totalAccessedNew = getNumTuplesAccessed(rt.getRoot());
            double AccessedNew = getNumTuplesAccessed(rt.getRoot(), q);
            System.out.println("Accessed tuple counts: " + AccessedOld + " what if update?: " + AccessedNew);

            double benefit = totalAccessedOld - totalAccessedNew;
            double cost = this.computeCost(rt.getRoot()) * WRITE_MULTIPLIER;

            System.out.println("Cost: " + cost + " benifit: " + benefit);

            if (benefit > cost) {
                updated = true;
            } else {
                rt.unmarshall(oldJoinRobustTree);
            }
        }


        PartitionSplit[] psplits;

        if (updated) {
            if(fullRepartition){
                psplits = this.getPartitionSplits(q, allNodes);
            }
            else {
                psplits = this.getPartitionSplits(q, nodes);
            }

        } else {
            psplits = this.buildAccessPlan(q);
        }


        this.persistQueryToDisk(q);
        if (updated) {
            System.out.println("INFO: persist index to disk");
            updateBucketIds(rt.getRoot());
            persistIndexToDisk();
            for (int i = 0; i < psplits.length; i++) {
                if (psplits[i].getIterator().getClass() == RepartitionIterator.class) {
                    psplits[i] = new PartitionSplit(
                            psplits[i].getPartitions(),
                            new RepartitionIterator(q.castToQuery()));
                }
            }
        } else {
            System.out.println("INFO: No index update");
        }

        return psplits;
    }

    private int minFullyAccessedDepth(JRNode node, int depth) {
        if (node.bucket != null) {
            return depth;
        }

        if (node.fullAccessed) {
            return depth;
        }

        int d1 = minFullyAccessedDepth(node.leftChild, depth + 1);
        int d2 = minFullyAccessedDepth(node.rightChild, depth + 1);

        if (d1 < d2) {
            return d1;
        } else {
            return d2;
        }
    }

    private void adjustJoinRobustTree(List<Predicate> choices, JoinQuery q) {
        Predicate[] ps = q.getPredicates();
        Collections.shuffle(choices);

        for (Predicate p : choices) {
            adjustJoinRobustTreeForPredicate(rt.getRoot(), p, ps, 1);
        }
    }


    private void getPartitionSplits_helper(JRNode node, List<Integer> unmodifiedBuckets, List<Integer> modifiedBuckets) {
        if (node.bucket != null) {
            if (node.fullAccessed && node.updated == false) {
                unmodifiedBuckets.add(node.bucket.getBucketId());
            } else if (node.fullAccessed && node.updated == true) {
                modifiedBuckets.add(node.bucket.getBucketId());
            }
        } else {
            getPartitionSplits_helper(node.leftChild, unmodifiedBuckets, modifiedBuckets);
            getPartitionSplits_helper(node.rightChild, unmodifiedBuckets, modifiedBuckets);
        }
    }

    private PartitionSplit[] getPartitionSplits(JoinQuery q, List<JRNode> nodes) {
        List<Integer> unmodifiedBuckets = new ArrayList<Integer>();
        List<Integer> modifiedBuckets = new ArrayList<Integer>();

        for(JRNode node : nodes){
            if (node.updated == false){
                unmodifiedBuckets.add(node.bucket.getBucketId());
            }else{
                modifiedBuckets.add(node.bucket.getBucketId());
            }
        }

        PartitionSplit[] splits = new PartitionSplit[2];
        int[] unmodifiedBucketsIds = new int[unmodifiedBuckets.size()];
        for (int i = 0; i < unmodifiedBucketsIds.length; i++) {
            unmodifiedBucketsIds[i] = unmodifiedBuckets.get(i);
        }
        PartitionIterator pi = new PostFilterIterator(q.castToQuery());
        splits[0] = new PartitionSplit(unmodifiedBucketsIds, pi);


        int[] modifiedBucketsIds = new int[modifiedBuckets.size()];
        for (int i = 0; i < modifiedBucketsIds.length; i++) {
            modifiedBucketsIds[i] = modifiedBuckets.get(i);
        }
        PartitionIterator pj = new JoinRepartitionIterator(q.castToQuery());
        splits[1] = new PartitionSplit(modifiedBucketsIds, pj);

        return splits;
    }

    /**
     * Puts the new estimated number of tuples in each bucket after change
     *
     * @param changed
     */
    public void populateBucketEstimates(JRNode changed) {
        ParsedTupleList collector = null;
        double numTuples = 0;
        int numSamples = 0;

        LinkedList<JRNode> stack = new LinkedList<JRNode>();
        stack.add(changed);

        while (stack.size() > 0) {
            JRNode n = stack.removeLast();
            if (n.bucket != null) {
                ParsedTupleList bucketSample = n.bucket.getSample();
                if (collector == null) {
                    collector = new ParsedTupleList(bucketSample.getTypes());
                }

                numSamples += bucketSample.getValues().size();
                numTuples += n.bucket.getEstimatedNumTuples();
                collector.addValues(bucketSample.getValues());
            } else {
                stack.add(n.rightChild);
                stack.add(n.leftChild);
            }
        }

        populateBucketEstimates(changed, collector);
    }

    private void populateBucketEstimates(JRNode n, ParsedTupleList sample) {
        if (n.bucket != null) {
            n.bucket.setEstimatedNumTuples(1.0 * sample.size() / rt.sample.size() * tableInfo.numTuples);
            n.bucket.setSample(sample);
        } else {
            // By sorting we avoid memory allocation
            // Will most probably be faster
            sample.sort(n.attribute);
            Pair<ParsedTupleList, ParsedTupleList> halves = sample
                    .splitAt(n.attribute, n.value);
            populateBucketEstimates(n.leftChild, halves.first);
            populateBucketEstimates(n.rightChild, halves.second);
        }
    }


    private boolean canAccessToThisNode(JRNode changed, JoinQuery q) {
        // First traverse to parent to see if query accesses node
        // If yes, find the number of tuples accessed.
        Predicate[] ps = q.getPredicates();

        JRNode node = changed;
        boolean accessed = true;
        while (node.parent != null) {
            for (Predicate p : ps) {
                if (p.attribute == node.parent.attribute) {
                    if (node.parent.leftChild == node) {
                        switch (p.predtype) {
                            case EQ:
                            case GEQ:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) > 0)
                                    accessed = false;
                                break;
                            case GT:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) >= 0)
                                    accessed = false;
                                break;
                            default:
                                break;
                        }
                    } else {
                        switch (p.predtype) {
                            case EQ:
                            case LEQ:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) <= 0)
                                    accessed = false;
                                break;
                            case LT:
                                if (TypeUtils.compareTo(p.value, node.parent.value,
                                        node.parent.type) < 0)
                                    accessed = false;
                                break;
                            default:
                                break;
                        }
                    }
                }

                if (!accessed)
                    break;
            }

            if (!accessed)
                break;
            node = node.parent;
        }
        return accessed;
    }

    /**
     * Gives the number of tuples accessed
     *
     * @param changed
     * @return
     */
    private double getNumTuplesAccessed(JRNode changed) {
        // First traverse to parent to see if query accesses node
        // If yes, find the number of tuples accessed.
        double numTuples = 0;

        for (int i = queryWindow.size() - 1; i >= 0; i--) {
            JoinQuery q = queryWindow.get(i);
            numTuples += getNumTuplesAccessed(changed, q);
        }

        return numTuples;
    }

    double getNumTuplesAccessed(JRNode changed, JoinQuery q) {

        if (canAccessToThisNode(changed, q)) {
            Predicate[] ps = q.getPredicates();

            List<JRNode> nodesAccessed = changed.search(ps);
            double tCount = 0;
            for (JRNode n : nodesAccessed) {
                tCount += n.bucket.getEstimatedNumTuples();
            }
            return tCount;
        }

        return 0;
    }


    private void updateBucketIds(JRNode node) {

        if (node.bucket != null) {
            if (node.updated) {
                node.bucket.updateId();
            }
            return;
        }
        updateBucketIds(node.leftChild);
        updateBucketIds(node.rightChild);
    }


    private boolean checkIfGoLeft(JRNode node, Predicate[] ps) {
        boolean goLeft = true;
        for (int i = 0; i < ps.length; i++) {
            Predicate pd = ps[i];
            if (pd.attribute == node.attribute) {
                switch (pd.predtype) {
                    case GEQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) > 0)
                            goLeft = false;
                        break;
                    case LEQ:
                        break;
                    case GT:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) >= 0)
                            goLeft = false;
                        break;
                    case LT:
                        break;
                    case EQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) > 0)
                            goLeft = false;
                        break;
                }
            }
        }
        return goLeft;
    }

    private boolean checkIfGoRight(JRNode node, Predicate[] ps) {
        boolean goRight = true;
        for (int i = 0; i < ps.length; i++) {
            Predicate pd = ps[i];
            if (pd.attribute == node.attribute) {
                switch (pd.predtype) {
                    case GEQ:
                        break;
                    case LEQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) <= 0)
                            goRight = false;
                        break;
                    case GT:
                        break;
                    case LT:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) <= 0)
                            goRight = false;
                        break;
                    case EQ:
                        if (TypeUtils
                                .compareTo(pd.value, node.value, node.type) <= 0)
                            goRight = false;
                        break;
                }
            }
        }
        return goRight;
    }

    private void partitionSubTreeByJoinAttribute(JRNode node, int joinAttribute, ParsedTupleList sample) {
        // grab the median, change the atrr and value on the node, then recurse.
        if (node.bucket != null) {
            node.updated = true;
            return;
        }

        sample.sort(joinAttribute);

        List<Object[]> values = sample.getValues();
        Object medianVal = values.get(values.size() / 2)[joinAttribute];

        node.attribute = joinAttribute;
        node.value = medianVal;
        node.type = sample.getTypes()[joinAttribute];

        Pair<ParsedTupleList, ParsedTupleList> halves = sample.splitByMedian(joinAttribute);
        partitionSubTreeByJoinAttribute(node.leftChild, joinAttribute, halves.first);
        partitionSubTreeByJoinAttribute(node.rightChild, joinAttribute, halves.second);
    }


    private boolean isBottomLevelNode(JRNode node) {
        return node.leftChild.bucket != null && node.rightChild.bucket != null;
    }
    private boolean isNotBottomLevelNode(JRNode node){
        return node.leftChild.bucket == null && node.rightChild.bucket == null;
    }


    private void adjustJoinRobustTreeForPredicate(JRNode node, Predicate choice, Predicate[] ps, int depth) {
        // Option Index
        // 1 => Replace
        // 2 => Swap down X

        if (node.bucket != null) {
            // Leaf
            node.fullAccessed = true;
            return;
        }

        Predicate p = choice;

        // Check if both sides are accessed
        boolean goLeft = checkIfGoLeft(node, ps);
        boolean goRight = checkIfGoRight(node, ps);



        //System.out.println("attribute: " + node.attribute + " value: " + node.value + " goleft: " + goLeft + " goRight: " + goRight);

        if (goLeft) {
            adjustJoinRobustTreeForPredicate(node.leftChild, choice, ps, depth + 1);
        }

        if (goRight) {
            adjustJoinRobustTreeForPredicate(node.rightChild, choice, ps, depth + 1);
        }

        if (depth > this.rt.joinAttributeDepth) {
            if (isBottomLevelNode(node)) {
                if (node.leftChild.fullAccessed && node.rightChild.fullAccessed) {
                    node.fullAccessed = true;

                    // When trying to replace by predicate;
                    // Replace by testVal, not the actual predicate value
                    Object testVal = p.getHelpfulCutpoint();

                    // replace attribute by one in the predicate

                    // If we traverse to root and see that there is no node with
                    // cutoff point less than
                    // that of predicate, we can do this

                    double numAccessedOld = getNumTuplesAccessed(node);

                    int attribute = node.attribute;
                    Object value = node.value;
                    TYPE type = node.type;

                    node.attribute = p.attribute;
                    node.type = p.type;
                    node.value = testVal;

                    populateBucketEstimates(node);
                    double numAcccessedNew = getNumTuplesAccessed(node);
                    double benefit = numAccessedOld - numAcccessedNew;

                    if (benefit > 0 && node.leftChild.bucket.getSample().size() != 0 && node.rightChild.bucket.getSample().size() != 0) {

                        node.leftChild.updated = true;
                        node.rightChild.updated = true;

                    } else {
                        // Restore ??

                        node.attribute = attribute;
                        node.type = type;
                        node.value = value;

                        populateBucketEstimates(node);
                    }
                }

            } else  if (isNotBottomLevelNode(node)){
                // Swap down the attribute and bring p above

                if (node.leftChild.fullAccessed && node.rightChild.fullAccessed) {
                    node.fullAccessed = true;

                }

                if (node.leftChild.attribute == node.rightChild.attribute &&
                        node.leftChild.value.equals(node.rightChild.value)) {

                    int attribute = node.attribute;
                    Object value = node.value;
                    TYPE type = node.type;

                    JRNode ll = node.leftChild.leftChild;
                    JRNode lr = node.leftChild.rightChild;
                    JRNode rl = node.rightChild.leftChild;
                    JRNode rr = node.rightChild.rightChild;

                    // swap lr and rl

                    node.leftChild.rightChild = rl;
                    node.rightChild.leftChild = lr;

                    rl.parent = node.leftChild;
                    lr.parent = node.rightChild;

                    // some condition here

                    node.attribute = node.leftChild.attribute;
                    node.value = node.leftChild.value;
                    node.type = node.leftChild.type;


                    node.leftChild.attribute = attribute;
                    node.leftChild.value = value;
                    node.leftChild.type = type;

                    node.rightChild.attribute = attribute;
                    node.rightChild.value = value;
                    node.rightChild.type = type;

                    populateBucketEstimates(node);

                }

                    /*
                    else if (node.rightChild.attribute == node.attribute){ // left rotate

                        JRNode r = node.rightChild;
                        JRNode rl = node.rightChild.leftChild;
                        JRNode parnet = node.parent;
                        node.rightChild = rl;
                        node.parent = r;
                        r.leftChild = node;
                        r.parent = parnet;
                        return r;

                    } else if (node.leftChild.attribute == node.attribute) { // right rotate
                        JRNode l = node.leftChild;
                        JRNode lr = node.leftChild.rightChild;
                        JRNode parnet = node.parent;
                        node.leftChild = lr;
                        node.parent = l;
                        l.rightChild = node;
                        l.parent = parnet;
                        return l;
                    }
                    */


            }
        } else {
            if (node.leftChild.fullAccessed && node.rightChild.fullAccessed) {
                node.fullAccessed = true;
            }
        }

    }

    public void checkNotEmpty(JRNode r){
        if(r.bucket !=null){
            if (r.bucket.getSample().size() == 0){
                System.out.println(r.bucket.getBucketId());
            }
            return;
        }
        checkNotEmpty(r.leftChild);
        checkNotEmpty(r.rightChild);
    }

    private double computeCost(JRNode r) {
        if (r.bucket != null) {
            if (r.updated) {
                return r.bucket.getEstimatedNumTuples();
            } else {
                return 0;
            }
        }
        double d1 = computeCost(r.leftChild);
        double d2 = computeCost(r.rightChild);

        return d1 + d2;
    }

    public void loadQueries(TableInfo tableInfo) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        String pathToQueries = this.workingDir + "/" + tableInfo.tableName + "/queries";
        try {
            if (fs.exists(new Path(pathToQueries))) {
                byte[] queryBytes = HDFSUtils.readFile(fs, pathToQueries);
                String queries = new String(queryBytes);
                Scanner sc = new Scanner(queries);
                while (sc.hasNextLine()) {
                    String query = sc.nextLine();
                    JoinQuery f = new JoinQuery(query);
                    queryWindow.add(f);
                }

                if (queryWindow.size() > 10) {
                    // set windows size
                    queryWindow = queryWindow.subList(queryWindow.size() - 10, queryWindow.size());
                }


                sc.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void persistQueryToDisk(JoinQuery q) {
        String pathToQueries = this.workingDir + "/" + q.getTable() + "/queries";
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        HDFSUtils.safeCreateFile(fs, pathToQueries,
                this.fileReplicationFactor);
        HDFSUtils.appendLine(fs, pathToQueries, q.toString());
    }


    private void persistIndexToDisk() {
        String pathToIndex = this.workingDir + "/" + rt.tableInfo.tableName + "/index";
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        try {
            if (fs.exists(new Path(pathToIndex))) {
                // If index file exists, move it to a new filename
                long currentMillis = System.currentTimeMillis();
                String oldIndexPath = pathToIndex + "." + currentMillis;
                boolean successRename = fs.rename(new Path(pathToIndex),
                        new Path(oldIndexPath));
                if (!successRename) {
                    System.out.println("Index rename to " + oldIndexPath
                            + " failed");
                }
            }
            HDFSUtils.safeCreateFile(fs, pathToIndex,
                    this.fileReplicationFactor);
        } catch (IOException e) {
            System.out.println("ERR: Writing Index failed: " + e.getMessage());
            e.printStackTrace();
        }

        byte[] indexBytes = this.rt.marshall();
        HDFSUtils.writeFile(fs,
                pathToIndex, this.fileReplicationFactor, indexBytes, 0,
                indexBytes.length, false);
    }
}
