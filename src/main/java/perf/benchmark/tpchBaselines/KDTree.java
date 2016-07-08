package perf.benchmark.TPCHBaselines;

import core.common.globals.TableInfo;
import core.common.index.RNode;
import core.common.index.RobustTree;
import core.common.key.ParsedTupleList;
import core.utils.Pair;

import java.util.LinkedList;

/**
 * Implements a KD Tree.
 * Assigns attribute randomly to each level in the tree.
 */
public class KDTree extends RobustTree {
    public KDTree(TableInfo ti) {
        super(ti);
    }

    @Override
    public void initProbe() {
        // Computes log(this.maxBuckets)
        int maxDepth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets);
        System.out.println("Tree Depth: " + maxDepth);

        // Assumes number of levels less than number of attributes.

        // Initialize root with attribute 0
        LinkedList<Task> nodeQueue = new LinkedList<Task>();
        Task initialTask = new Task();
        initialTask.node = root;
        initialTask.sample = this.sample;
        initialTask.depth = 0;
        nodeQueue.add(initialTask);

        while (nodeQueue.size() > 0) {
            Task t = nodeQueue.pollFirst();
            if (t.depth < maxDepth) {
                Pair<ParsedTupleList, ParsedTupleList> halves = null;
                int dim = t.depth;
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
    }
}
