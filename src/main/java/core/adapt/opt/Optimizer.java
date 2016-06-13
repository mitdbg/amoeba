package core.adapt.opt;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.Predicate;
import core.adapt.Query;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.iterator.RepartitionIterator;
import core.adapt.spark.SparkQueryConf;
import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.index.MDIndex.Bucket;
import core.common.index.RNode;
import core.common.index.RobustTree;
import core.common.key.ParsedTupleList;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.Pair;
import core.utils.TypeUtils;
import core.utils.TypeUtils.TYPE;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

/**
 * Optimizer creates the execution plans for the queries.
 * It uses the incoming query predicates as hints for what should be added
 * into the partitioning tree. If we find a plan which has benefit >
 * cost, we do the repartitioning. Else we just do a scan.
 *
 * @author anil
 */
public class Optimizer {
    private static final double WRITE_MULTIPLIER = 1.5;

    private RobustTree rt;

    // Properties extracted from ConfUtils
    private String workingDir;
    private String hadoopHome;
    private short fileReplicationFactor;

    private List<Query> queryWindow = new ArrayList<Query>();

    public Optimizer(SparkQueryConf cfg) {
        // Working Directory for the Optimizer.
        // Each table is a folder under this directory.
        this.workingDir = cfg.getWorkingDir();
        this.hadoopHome = cfg.getHadoopHome();
        this.fileReplicationFactor = cfg.getHDFSReplicationFactor();
    }

    public Optimizer(ConfUtils cfg) {
        this.workingDir = cfg.getHDFS_WORKING_DIR();
        this.hadoopHome = cfg.getHADOOP_HOME();
        this.fileReplicationFactor = cfg.getHDFS_REPLICATION_FACTOR();
    }

    /**
     * Checks if it is valid to partition on attrId with value val at node
     *
     * @param node
     * @param attrId
     * @param t
     * @param val
     * @return
     */
    public static boolean checkValidToRoot(RNode node, int attrId, TYPE t,
                                           Object val) {
        while (node.parent != null) {
            if (node.parent.attribute == attrId) {
                int ret = TypeUtils.compareTo(node.parent.value, val, t);
                if (node.parent.leftChild == node && ret <= 0) {
                    return false;
                } else if (node.parent.rightChild == node && ret >= 0) {
                    return false;
                }
            }

            node = node.parent;
        }

        return true;
    }

    static float getNumTuplesAccessed(RNode changed, Query q) {
        // First traverse to parent to see if query accesses node
        // If yes, find the number of tuples accessed.
        Predicate[] ps = q.getPredicates();

        RNode node = changed;
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
                                        node.parent.type) <= 0)
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

        List<RNode> nodesAccessed = changed.search(ps);
        float tCount = 0;
        for (RNode n : nodesAccessed) {
            tCount += n.bucket.getEstimatedNumTuples();
        }

        return tCount;
    }

    public void loadIndex(TableInfo tableInfo) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        String tableDir = this.workingDir + "/" + tableInfo.tableName;
        String pathToIndex = tableDir + "/index";
        String pathToSample = tableDir + "/sample";

        byte[] indexBytes = HDFSUtils.readFile(fs, pathToIndex);
        this.rt = new RobustTree(tableInfo);
        this.rt.unmarshall(indexBytes);

        byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
        this.rt.loadSample(tableInfo, sampleBytes);
    }

    public RobustTree getIndex() {
        return rt;
    }

    public int[] getBidFromRNodes(List<RNode> nodes) {
        int[] bids = new int[nodes.size()];
        Iterator<RNode> it = nodes.iterator();
        for (int i = 0; i < bids.length; i++) {
            bids[i] = it.next().bucket.getBucketId();
        }
        return bids;
    }

    public PartitionSplit[] buildAccessPlan(final Query fq) {
        List<RNode> nodes = this.rt.getRoot().search(fq.getPredicates());

        double tcost = 0;
        for (RNode r : nodes) {
            tcost += r.bucket.getEstimatedNumTuples();
        }

        System.out.println("INFO: Total Cost " + tcost);

        PartitionIterator pi = new PostFilterIterator(fq);
        int[] bids = this.getBidFromRNodes(nodes);

        PartitionSplit psplit = new PartitionSplit(bids, pi);
        PartitionSplit[] ps = new PartitionSplit[1];
        ps[0] = psplit;
        return ps;
    }

    /**
     * Build a plan that incorporates multiple predicates.
     *
     * @param q
     * @return
     */
    public PartitionSplit[] buildMultiPredicatePlan(final Query q) {
        System.out.println("INFO: Running query " + q.toString());
        this.queryWindow.add(q);

        Predicate[] ps = q.getPredicates();
        LinkedList<Predicate> choices = new LinkedList<Predicate>();

        // Initialize the set of choices for predicates.
        for (int i = 0; i < ps.length; i++) {
            choices.add(ps[i]);
        }

        double benefit = 0;
        List<RNode> buckets = rt.getMatchingBuckets(ps);

        // Till we get no better plan, try to add a predicate into the tree.
        List<Predicate> predicatesInserted = new ArrayList<>();
        while (true) {
            Plan best = getBestPlan(choices, ps);
            if (best == null) {
                break;
            }

            Predicate inserted = getPredicateInserted(best);
            if (inserted == null) {
                break;
            } else {
                benefit += best.benefit;
                this.updateIndex(best, q.getPredicates());
                choices.remove(inserted);
                predicatesInserted.add(inserted);
            }
        }

        List<RNode> newBuckets = rt.getMatchingBuckets(ps);
        double cost = 0;
        double tcost = 0;
        List<Integer> modifiedBuckets = new ArrayList<>();
        List<Integer> unmodifiedBuckets = new ArrayList<>();

        // TODO: Maybe just sort ?
        for (RNode r : buckets) {
            boolean found = false;
            for (RNode s : newBuckets) {
                if (s.bucket.getBucketId() == r.bucket.getBucketId()) {
                    found = true;
                    break;
                }
            }

            if (found) {
                unmodifiedBuckets.add(r.bucket.getBucketId());
                tcost += r.bucket.getEstimatedNumTuples();
            } else {
                modifiedBuckets.add(r.bucket.getBucketId());
                cost += Globals.c * r.bucket.getEstimatedNumTuples();
                tcost += Globals.c * r.bucket.getEstimatedNumTuples();
            }
        }

        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        this.persistQueryToDisk(fs, q);
        List<PartitionSplit> lps = new ArrayList<>();
        System.out.println("INFO: Benefit " + benefit + " Cost " + cost + " Total Cost " + tcost);
        if (benefit > cost) {
            if (unmodifiedBuckets.size() > 0) {
                PartitionIterator pi = new PostFilterIterator(q);
                int[] bids = new int[unmodifiedBuckets.size()];
                int counter = 0;
                for (Integer i : unmodifiedBuckets) {
                    bids[counter] = i;
                    counter++;
                }
                PartitionSplit psplit = new PartitionSplit(bids, pi);
                lps.add(psplit);
            }

            if (modifiedBuckets.size() > 0) {
                PartitionIterator pi = new RepartitionIterator(q);
                int[] bids = new int[modifiedBuckets.size()];
                int counter = 0;
                for (Integer i : modifiedBuckets) {
                    bids[counter] = i;
                    counter++;
                }
                PartitionSplit psplit = new PartitionSplit(bids, pi);
                lps.add(psplit);

                System.out.println("INFO: Predicates inserted: " + predicatesInserted.toString());

                System.out.println("INFO: Index being updated");
                this.persistIndexToDisk(fs);
            }
        } else {
            PartitionIterator pi = new PostFilterIterator(q);
            int[] bids = new int[unmodifiedBuckets.size() + modifiedBuckets.size()];
            int counter = 0;
            for (Integer i : unmodifiedBuckets) {
                bids[counter] = i;
                counter++;
            }
            for (Integer i : modifiedBuckets) {
                bids[counter] = i;
                counter++;
            }
            PartitionSplit psplit = new PartitionSplit(bids, pi);
            lps.add(psplit);
        }

        PartitionSplit[] splits = lps.toArray(new PartitionSplit[lps.size()]);
        return splits;
    }

    private Predicate getPredicateInserted(Plan plan) {
        LinkedList<Action> queue = new LinkedList<Action>();
        queue.add(plan.actions);
        Predicate pred = null;
        while (!queue.isEmpty()) {
            Action top = queue.removeFirst();
            if (top.option == 1) {
                pred = top.pid;
                break;
            }

            if (top.left != null) {
                queue.add(top.left);
            }

            if (top.right != null) {
                queue.add(top.right);
            }
        }
        return pred;
    }

    public PartitionSplit[] buildPlan(final Query q) {
        this.queryWindow.add(q);

        Predicate[] ps = q.getPredicates();
        LinkedList<Predicate> choices = new LinkedList<Predicate>();

        // Initialize the set of choices for predicates.
        for (int i = 0; i < ps.length; i++) {
            choices.add(ps[i]);
        }

        Plan best = getBestPlan(choices, ps);

        System.out.println("plan.cost: " + best.cost + " plan.benefit: "
                + best.benefit);
        if (best.cost > best.benefit) {
            best = null;
        }

        PartitionSplit[] psplits;
        if (best != null) {
            psplits = this.getPartitionSplits(best, q);
        } else {
            psplits = this.buildAccessPlan(q);
        }

        // Check if we are updating the index ?
        boolean updated = true;
        if (psplits.length == 1) {
            if (psplits[0].getIterator().getClass() == PostFilterIterator.class) {
                updated = false;
            }
        }

        // Debug
        long totalCostOfQuery = 0;
        for (int i = 0; i < psplits.length; i++) {
            int[] bids = psplits[i].getPartitions();
            double numTuplesAccessed = 0;
            for (int j = 0; j < bids.length; j++) {
                numTuplesAccessed += Globals.c * Bucket.getEstimatedNumTuples(bids[j]);
            }

            totalCostOfQuery += numTuplesAccessed;
        }
        System.out.println("Query Cost: " + totalCostOfQuery);

        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        this.persistQueryToDisk(fs, q);
        if (updated) {
            System.out.println("INFO: Index being updated");
            this.updateIndex(best, q.getPredicates());
            this.persistIndexToDisk(fs);
        } else {
            System.out.println("INFO: No index update");
        }

        return psplits;
    }

    private Plan getBestPlan(List<Predicate> choices, Predicate[] ps) {
        Plan plan = null;
        for (Predicate p : choices) {
            Plan option = getBestPlanForPredicate(p, ps);
            if (plan == null) {
                plan = option;
            } else {
                this.updatePlan(plan, option);
            }
        }

        return plan;
    }

    private void updateIndex(Plan best, Predicate[] ps) {
        this.applyActions(this.rt.getRoot(), best.actions, ps);
    }

    private void applyActions(RNode n, Action a, Predicate[] ps) {
        boolean isRoot = false;
        if (n.parent == null)
            isRoot = true;

        if (a.left != null) {
            this.applyActions(n.leftChild, a.left, ps);
        }

        if (a.right != null) {
            this.applyActions(n.rightChild, a.right, ps);
        }

        switch (a.option) {
            case 0:
                break;
            case 1:
                Predicate p = a.pid;
                RNode r = n.clone();
                r.attribute = p.attribute;
                r.type = p.type;
                r.value = p.getHelpfulCutpoint();
                replaceInTree(n, r);

                // This is the root
                if (isRoot)
                    rt.setRoot(r);

                break;
            case 2:
                Predicate p2 = a.pid;
                assert p2.attribute == n.leftChild.attribute;
                assert p2.attribute == n.rightChild.attribute;
                RNode n2 = n.clone();
                RNode right = n.rightChild.clone();
                replaceInTree(n.leftChild, n2);
                replaceInTree(n, right);
                replaceInTree(right.rightChild, n);
                if (isRoot)
                    rt.setRoot(right);
                break;
            case 3:
                assert a.right == null || a.left == null;
                if (a.left == null) {
                    RNode t = n.rightChild;
                    RNode tr = t.rightChild;
                    RNode tl = t.leftChild;

                    replaceInTree(n, t);
                    t.rightChild = tr;
                    t.rightChild.parent = t;
                    t.leftChild = n;
                    t.leftChild.parent = t;
                    // As side-effect of replace, n.leftChild.parent points to t
                    n.leftChild.parent = n;

                    n.rightChild = tl;
                    n.rightChild.parent = n;

                    if (isRoot)
                        rt.setRoot(t);
                } else {
                    RNode t = n.leftChild;
                    RNode tr = t.rightChild;
                    RNode tl = t.leftChild;

                    replaceInTree(n, t);
                    t.leftChild = tl;
                    t.leftChild.parent = t;
                    t.rightChild = n;
                    t.rightChild.parent = t;
                    n.rightChild.parent = n;
                    n.leftChild = tr;
                    n.leftChild.parent = n;

                    if (isRoot)
                        rt.setRoot(t);
                }
                break;
            case 4:
                break;
            case 5:
                break;
        }
    }

    private PartitionSplit[] getPartitionSplits(Plan best, Query fq) {
        List<PartitionSplit> lps = new ArrayList<PartitionSplit>();
        final int[] modifyingOptions = new int[]{1};
        Action acTree = best.actions;

        LinkedList<RNode> nodeStack = new LinkedList<RNode>();
        nodeStack.add(this.rt.getRoot());

        LinkedList<Action> actionStack = new LinkedList<Action>();
        actionStack.add(acTree);

        List<Integer> unmodifiedBuckets = new ArrayList<Integer>();

        boolean printed = false;

        Predicate[] ps = fq.getPredicates();
        while (nodeStack.size() > 0) {
            RNode n = nodeStack.removeLast();
            Action a = actionStack.removeLast();

            boolean isModifying = false;
            for (int t : modifyingOptions) {
                if (t == a.option) {
                    List<RNode> bs = n.search(ps);
                    int[] bucketIds = new int[bs.size()];
                    for (int i = 0; i < bucketIds.length; i++) {
                        bucketIds[i] = bs.get(i).bucket.getBucketId();
                    }

                    Predicate p = a.pid;
                    RNode r = n.clone();
                    r.attribute = p.attribute;
                    r.type = p.type;
                    r.value = p.value;
                    replaceInTree(n, r);

                    if (!printed) {
                        System.out.println("Inserted pred: " + p.toString());
                        printed = true;
                    }

                    // Give new bucket ids to all nodes below this
                    updateBucketIds(bs);

                    PartitionIterator pi = new RepartitionIterator(fq);
                    PartitionSplit psplit = new PartitionSplit(bucketIds, pi);
                    lps.add(psplit);
                    isModifying = true;
                    break;
                }
            }

            if (!isModifying) {
                if (a.right != null) {
                    actionStack.add(a.right);
                    nodeStack.add(n.rightChild);
                }

                if (a.left != null) {
                    actionStack.add(a.left);
                    nodeStack.add(n.leftChild);
                }

                if (n.bucket != null) {
                    unmodifiedBuckets.add(n.bucket.getBucketId());
                }
            }
        }

        if (unmodifiedBuckets.size() > 0) {
            PartitionIterator pi = new PostFilterIterator(fq);
            int[] bids = new int[unmodifiedBuckets.size()];
            Iterator<Integer> it = unmodifiedBuckets.iterator();
            for (int i = 0; i < bids.length; i++) {
                bids[i] = it.next();
            }
            PartitionSplit psplit = new PartitionSplit(bids, pi);
            lps.add(psplit);
        }

        PartitionSplit[] splits = lps.toArray(new PartitionSplit[lps.size()]);
        return splits;
    }

    /**
     * Checks if it is valid to partition on attrId with value val at node's
     * parent
     *
     * @param node
     * @param attrId
     * @param t
     * @param val
     * @param isLeft - indicates if node is to the left(1) or right(-1) of parent
     * @return
     */
    private boolean checkValidForSubtree(RNode node, int attrId, TYPE t,
                                         Object val, int isLeft) {
        LinkedList<RNode> stack = new LinkedList<RNode>();
        stack.add(node);

        while (stack.size() > 0) {
            RNode n = stack.removeLast();
            if (n.bucket == null) {
                if (n.attribute == attrId) {
                    int comp = TypeUtils.compareTo(n.value, val, t);
                    if (comp * isLeft >= 0)
                        return false;
                }
                stack.add(n.rightChild);
                stack.add(n.leftChild);
            }
        }
        return true;
    }

    /**
     * Puts the new estimated number of tuples in each bucket after change
     *
     * @param changed
     */
    private void populateBucketEstimates(RNode changed) {
        ParsedTupleList collector = null;

        LinkedList<RNode> stack = new LinkedList<RNode>();
        stack.add(changed);

        while (stack.size() > 0) {
            RNode n = stack.removeLast();
            if (n.bucket != null) {
                ParsedTupleList bucketSample = n.bucket.getSample();
                if (collector == null) {
                    collector = new ParsedTupleList(bucketSample.getTypes());
                }

                collector.addValues(bucketSample.getValues());
            } else {
                stack.add(n.rightChild);
                stack.add(n.leftChild);
            }
        }

        populateBucketEstimates(changed, collector);
    }

    private void populateBucketEstimates(RNode n, ParsedTupleList sample) {
        if (n.bucket != null) {
            n.bucket.setEstimatedNumTuples(((1.0 * sample.size()) / rt.sample.size()) * rt.tableInfo.numTuples);
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

    /**
     * Gives the number of tuples accessed
     *
     * @param changed
     * @return
     */
    private double getNumTuplesAccessed(RNode changed) {
        // First traverse to parent to see if query accesses node
        // If yes, find the number of tuples accessed.
        double numTuples = 0;

        // Access the last 20 queries at max.
        for (int i = queryWindow.size() - 1; i >= Math.max(queryWindow.size() - Globals.window_size, 0); i--) {
            Query q = queryWindow.get(i);
            numTuples += getNumTuplesAccessed(changed, q);
        }

        return numTuples;
    }

    /**
     * Replaces node old by node r in the tree
     *
     * @param old
     * @param r
     */
    private void replaceInTree(RNode old, RNode r) {
        old.leftChild.parent = r;
        old.rightChild.parent = r;
        if (old.parent != null) {
            if (old.parent.rightChild == old) {
                old.parent.rightChild = r;
            } else {
                old.parent.leftChild = r;
            }
        }

        r.leftChild = old.leftChild;
        r.rightChild = old.rightChild;
        r.parent = old.parent;
    }

    private Plan getBestPlanForPredicate(Predicate choice, Predicate[] ps) {
        RNode root = rt.getRoot();
        Plans plans = getBestPlanForSubtree(root, choice, ps);
        return plans.Best;
    }

    private void updateBucketIds(List<RNode> r) {
        for (RNode n : r) {
            n.bucket.updateId();
        }
    }

    // Update the dest with source if source is a better plan
    private void updatePlan(Plan dest, Plan source) {
        boolean copy = false;
        if (dest.benefit == -1) {
            copy = true;
        } else if (source.cost == 0) {

        } else if (dest.cost == 0) {
            copy = true;
        } else if (dest.benefit / dest.cost < source.benefit / source.cost) {
            copy = true;
        }

        if (copy) {
            dest.benefit = source.benefit;
            dest.cost = source.cost;
            dest.actions = source.actions;
        }
    }

    private Plans getBestPlanForSubtree(RNode node, Predicate choice, Predicate[] ps) {
        // Option Index
        // 1 => Replace
        // 2 => Swap down X
        // 3 =>
        // 5 => Reuse the best plan of subtrees

        if (node.bucket != null) {
            // Leaf
            Plans pl = new Plans();
            pl.fullAccess = true;
            Plan p1 = new Plan();
            p1.cost = 0;
            p1.benefit = 0;
            Action a1 = new Action();
            a1.option = 5;
            p1.actions = a1;
            pl.Best = p1;
            return pl;
        } else {
            Predicate p = choice;
            Plan pTop = new Plan();

            Plan best = new Plan();

            Plans ret = new Plans();

            // Check if both sides are accessed
            boolean goLeft = true;
            boolean goRight = true;
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
                            if (TypeUtils
                                    .compareTo(pd.value, node.value, node.type) <= 0)
                                goRight = false;
                            break;
                        case GT:
                            if (TypeUtils
                                    .compareTo(pd.value, node.value, node.type) >= 0)
                                goLeft = false;
                            break;
                        case LT:
                            if (TypeUtils
                                    .compareTo(pd.value, node.value, node.type) < 0)
                                goRight = false;
                            break;
                        case EQ:
                            if (TypeUtils
                                    .compareTo(pd.value, node.value, node.type) <= 0)
                                goRight = false;
                            else
                                goLeft = false;
                            break;
                    }
                }
            }

            Plans leftPlan;
            if (goLeft) {
                leftPlan = getBestPlanForSubtree(node.leftChild, choice, ps);
            } else {
                leftPlan = new Plans();
                leftPlan.Best = null;
                leftPlan.PTop = null;
                leftPlan.fullAccess = false;
            }

            Plans rightPlan;
            if (goRight) {
                rightPlan = getBestPlanForSubtree(node.rightChild, choice, ps);
            } else {
                rightPlan = new Plans();
                rightPlan.Best = null;
                rightPlan.PTop = null;
                rightPlan.fullAccess = false;
            }

            // When trying to replace by predicate;
            // Replace by testVal, not the actual predicate value
            Object testVal = p.getHelpfulCutpoint();

            // replace attribute by one in the predicate
            if (leftPlan.fullAccess && rightPlan.fullAccess) {
                ret.fullAccess = true;

                // If we traverse to root and see that there is no node with
                // cutoff point less than
                // that of predicate, we can do this
                if (checkValidToRoot(node, p.attribute, p.type, testVal)) {
                    double numAccessedOld = getNumTuplesAccessed(node);

                    RNode r = node.clone();
                    r.attribute = p.attribute;
                    r.type = p.type;
                    r.value = testVal;
                    replaceInTree(node, r);

                    populateBucketEstimates(r);
                    double numAccessedNew = getNumTuplesAccessed(r);
                    double benefit = numAccessedOld - numAccessedNew;

                    if (benefit > 0) {
                        // TODO: Better cost model ?
                        double cost = this.computeCost(r); // Note that buckets
                        // haven't changed
                        Plan pl = new Plan();
                        pl.cost = cost;
                        pl.benefit = benefit;
                        Action ac = new Action();
                        ac.pid = choice;
                        ac.option = 1;
                        pl.actions = ac;

                        updatePlan(pTop, pl);
                        updatePlan(best, pl);
                    }

                    // Restore
                    replaceInTree(r, node);
                    populateBucketEstimates(node);
                }
            }

            // Swap down the attribute and bring p above
            if (leftPlan.PTop != null && rightPlan.PTop != null) {
                Plan pl = new Plan();
                pl.cost = leftPlan.PTop.cost + rightPlan.PTop.cost;
                pl.benefit = leftPlan.PTop.benefit + rightPlan.PTop.benefit;
                Action ac = new Action();
                ac.pid = choice;
                ac.option = 2;
                ac.left = leftPlan.PTop.actions;
                ac.right = rightPlan.PTop.actions;
                pl.actions = ac;
                updatePlan(pTop, pl);
                updatePlan(best, pl);
            }

            if (node.attribute == p.attribute) {
                int c = TypeUtils.compareTo(node.value, testVal, node.type);
                if (c != 0) {
                    assert (c < 0 && leftPlan.PTop == null)
                            || (c > 0 && rightPlan.PTop == null);
                    // Rotate left
                    if (c < 0 && rightPlan.PTop != null) {
                        Plan pl = new Plan();
                        pl.cost = rightPlan.PTop.cost;
                        pl.benefit = rightPlan.PTop.benefit;
                        Action ac = new Action();
                        ac.pid = choice;
                        ac.option = 3;
                        ac.right = rightPlan.PTop.actions;
                        pl.actions = ac;
                        updatePlan(pTop, pl);
                        updatePlan(best, pl);
                    }

                    // Rotate right
                    if (c > 0 && leftPlan.PTop != null) {
                        Plan pl = new Plan();
                        pl.cost = leftPlan.PTop.cost;
                        pl.benefit = leftPlan.PTop.benefit;
                        Action ac = new Action();
                        ac.pid = choice;
                        ac.option = 3;
                        ac.left = leftPlan.PTop.actions;
                        pl.actions = ac;
                        updatePlan(pTop, pl);
                        updatePlan(best, pl);
                    }

                    // Replace by the predicate
                    // If we traverse to root and see that there is no node with
                    // cutoff point less than
                    // that of predicate,
                    if (checkValidToRoot(node, p.attribute, p.type, testVal)) {
                        boolean allGood;
                        if (c > 0) {
                            allGood = checkValidForSubtree(node.leftChild,
                                    p.attribute, p.type, testVal, 1);
                        } else {
                            allGood = checkValidForSubtree(node.rightChild,
                                    p.attribute, p.type, testVal, -1);
                        }

                        if (allGood) {
                            double numAccessedOld = getNumTuplesAccessed(node);

                            RNode r = node.clone();
                            r.attribute = p.attribute;
                            r.type = p.type;
                            r.value = testVal;
                            replaceInTree(node, r);

                            populateBucketEstimates(r);
                            double numAcccessedNew = getNumTuplesAccessed(r);
                            double benefit = numAccessedOld - numAcccessedNew;

                            if (benefit > 0) {
                                double cost = this.computeCost(r); // Note that
                                // buckets
                                // haven't
                                // changed
                                Plan pl = new Plan();
                                pl.cost = cost;
                                pl.benefit = benefit;
                                Action ac = new Action();
                                ac.pid = choice;
                                ac.option = 1;
                                pl.actions = ac;

                                updatePlan(pTop, pl);
                                updatePlan(best, pl);
                            }

                            // Restore
                            replaceInTree(r, node);
                        }
                    }
                }
            }

            // Just Re-Use the Best Plans found for the left/right subtree
            if (leftPlan.Best != null && rightPlan.Best != null) {
                Plan pl = new Plan();
                pl.cost = leftPlan.Best.cost + rightPlan.Best.cost;
                pl.benefit = leftPlan.Best.benefit + rightPlan.Best.benefit;
                Action ac = new Action();
                ac.pid = choice;
                ac.option = 5;
                ac.left = leftPlan.Best.actions;
                ac.right = rightPlan.Best.actions;
                pl.actions = ac;
                updatePlan(best, pl);
            } else if (rightPlan.Best != null) {
                Plan pl = new Plan();
                pl.cost = rightPlan.Best.cost;
                pl.benefit = rightPlan.Best.benefit;
                Action ac = new Action();
                ac.pid = choice;
                ac.option = 5;
                ac.right = rightPlan.Best.actions;
                pl.actions = ac;
                updatePlan(best, pl);
            } else if (leftPlan.Best != null) {
                Plan pl = new Plan();
                pl.cost = leftPlan.Best.cost;
                pl.benefit = leftPlan.Best.benefit;
                Action ac = new Action();
                ac.pid = choice;
                ac.option = 5;
                ac.left = leftPlan.Best.actions;
                pl.actions = ac;
                updatePlan(best, pl);
            }

            if (pTop.benefit != -1)
                ret.PTop = pTop;
            if (best.benefit != -1)
                ret.Best = best;

            return ret;
        }
    }

    private double computeCost(RNode r) {
        double numTuples = r.numTuplesInSubtree();
        return WRITE_MULTIPLIER * numTuples;
    }

    public void loadQueries() {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
        String pathToQueries = this.workingDir + "/queries";
        try {
            if (fs.exists(new Path(pathToQueries))) {
                byte[] queryBytes = HDFSUtils.readFile(fs, pathToQueries);
                String queries = new String(queryBytes);
                Scanner sc = new Scanner(queries);
                while (sc.hasNextLine()) {
                    String query = sc.nextLine();
                    Query f = new Query(query);
                    queryWindow.add(f);
                }
                sc.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void persistQueryToDisk(FileSystem fs, Query q) {
        String pathToQueries = this.workingDir + "/" + q.getTable() + "/queries";
        HDFSUtils.safeCreateFile(fs, pathToQueries,
                this.fileReplicationFactor);
        HDFSUtils.appendLine(fs, pathToQueries, q.toString());
    }

    private void persistIndexToDisk(FileSystem fs) {
        String pathToIndex = this.workingDir + "/" + rt.tableInfo.tableName + "/index";
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
                pathToIndex, this.fileReplicationFactor, this.rt.marshall(), 0,
                indexBytes.length, false);
    }

    private static class Plan {
        public Action actions; // Tree of actions
        public double cost;
        public double benefit;

        public Plan() {
            cost = 0;
            benefit = -1;
        }
    }

    public static class Action {
        public Predicate pid; // Predicate
        public int option; // Says which if I used to create this plan
        public Action left;
        public Action right;
    }

    public static class Plans {
        // Indicates if the entire subtree is accessed.
        boolean fullAccess;

        // TODO(anil): Think if this is needed at all.
        Plan PTop;

        // Best plan encountered for this subtree.
        Plan Best;

        public Plans() {
            this.fullAccess = false;
            this.PTop = null;
            this.Best = null;
        }
    }
}
