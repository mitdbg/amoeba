package perf.benchmark.baselines;

import core.common.globals.TableInfo;
import core.common.index.MDIndex;
import core.common.index.RNode;
import core.common.index.RobustTree;
import core.common.key.ParsedTupleList;
import core.utils.Pair;

import java.util.LinkedList;

/**
 * Implements a KD-Tree. With no workload information, each
 * level is assigned a random attribute.
 */
public class Range2Tree extends RobustTree {
    public Range2Tree(TableInfo ti) {
        super(ti);
    }

    public int getAttrForLevel(int level) {
       if (level < 6) {
           return tableInfo.schema.getAttributeId("o_orderdate");
       } else if (level < 8) {
           return tableInfo.schema.getAttributeId("c_region");
       } else if (level < 10) {
           return tableInfo.schema.getAttributeId("c_mktsegment");
       } else {
           return tableInfo.schema.getAttributeId("l_quantity");
       }
    }

    @Override
    public void initProbe() {
        // Computes log(this.maxBuckets)
        int maxDepth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets);
        System.out.println("Tree Depth: " + maxDepth);

        // Assumes number of levels less than number of attributes.

        // Initialize root with attribute 0
        LinkedList<RobustTree.Task> nodeQueue = new LinkedList<Task>();
        RobustTree.Task initialTask = new RobustTree.Task();
        initialTask.node = root;
        initialTask.sample = this.sample;
        initialTask.depth = 0;
        nodeQueue.add(initialTask);

        while (nodeQueue.size() > 0) {
            RobustTree.Task t = nodeQueue.pollFirst();
            if (t.depth < maxDepth) {
                Pair<ParsedTupleList, ParsedTupleList> halves = null;
                int dim = getAttrForLevel(t.depth);
                halves = t.sample.sortAndSplit(dim);

                if (halves.first.size() == 0 ||
                        halves.second.size() == 0) {
                    // Treat as no-op; go to next attribute
                    t.depth += 1;
                    nodeQueue.add(t);
                } else {
                    t.node.attribute = dim;
                    t.node.type = this.dimensionTypes[dim];
                    t.node.value = halves.first.getLast(dim); // Need to traverse up for range.

                    t.node.leftChild = new RNode();
                    t.node.leftChild.parent = t.node;
                    RobustTree.Task tl = new RobustTree.Task();
                    tl.node = t.node.leftChild;
                    tl.depth = t.depth + 1;
                    tl.sample = halves.first;
                    nodeQueue.add(tl);

                    t.node.rightChild = new RNode();
                    t.node.rightChild.parent = t.node;
                    RobustTree.Task tr = new RobustTree.Task();
                    tr.node = t.node.rightChild;
                    tr.depth = t.depth + 1;
                    tr.sample = halves.second;
                    nodeQueue.add(tr);
                }
            } else {
                MDIndex.Bucket b = new MDIndex.Bucket();
                b.setSample(sample);
                t.node.bucket = b;
            }
        }
    }
}
