package core.adapt;

import core.adapt.iterator.PartitionIterator;
import core.adapt.opt.Optimizer;
import core.adapt.spark.SparkQueryConf;
import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.index.RobustTree;
import core.common.key.RawIndexKey;
import core.utils.HDFSUtils;

/**
 * This access method class considers filter access method over the distributed
 * dataset. The filter could be extracted as: - the selection predicate in
 * selection query - the sub-range filter (different for each node) in
 * join/aggregate query
 * <p/>
 * Currently, we support filtering only on one attribute at a time, i.e. we
 * expect the query processor to filter on the most selective attribute.
 * <p/>
 * Filter query: - can access only the local blocks on each node - scan over
 * partitioned portion - crack over non-partitioned portion
 */

public class AccessMethod {
    Optimizer opt;
    RawIndexKey key;

    /**
     * Initialize hyper-partitioning data access.
     */
    public void init(SparkQueryConf conf) {
        Query query = conf.getQuery();

        Globals.loadTableInfo(query.getTable(), conf.getWorkingDir(),
                HDFSUtils.getFSByHadoopHome(conf.getHadoopHome()));

        TableInfo tableInfo = Globals.getTableInfo(query.getTable());
        key = new RawIndexKey(tableInfo.delimiter);
        opt = new Optimizer(conf);

        opt.loadIndex(tableInfo);
        opt.loadQueries();
    }

    public RobustTree getIndex() {
        return opt.getIndex();
    }

    public RawIndexKey getKey() {
        return key;
    }

    /**
     * This method returns whether or not a given partition qualifies for the
     * predicate.
     *
     * @param predicate
     * @return
     */
    public boolean isRelevant(String partitionid, Predicate predicate) {
        return true;
    }

    /**
     * This method is used to: 1. lookup the partition index for relevant
     * partitions 2. and, to create splits of partitions which could be assigned
     * to different node.
     * <p/>
     * The split thus produced must be: (a) equal in size (b) contain blocks
     * from the same sub-tree
     *
     * @return
     */
    public PartitionSplit[] getPartitionSplits(Query q, boolean justAccess) {
        if (justAccess) {
            return opt.buildAccessPlan(q);
        } else {
            return opt.buildPlan(q);
        }
    }

    /**
     * This class encapsulates a set of partitions which should be accessed in a
     * similar fashion. Each split gets assigned to a node as a whole.
     *
     * @author alekh
     */
    public static class PartitionSplit {
        private int[] partitionIds;
        private PartitionIterator iterator;

        public PartitionSplit(int[] partitionIds, PartitionIterator iterator) {
            this.partitionIds = partitionIds;
            this.iterator = iterator;
        }

        public int[] getPartitions() {
            return this.partitionIds;
        }

        public PartitionIterator getIterator() {
            return this.iterator;
        }
    }
}
