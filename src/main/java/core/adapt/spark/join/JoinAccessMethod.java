package core.adapt.spark.join;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.opt.JoinOptimizer;
import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.common.index.JoinRobustTree;
import core.common.key.RawIndexKey;
import core.utils.HDFSUtils;

/**
 * Created by ylu on 1/27/16.
 */

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

public class JoinAccessMethod {
    public JoinOptimizer opt;
    public RawIndexKey key;

    /**
     * Initialize hyper-partitioning data access.
     */
    public void init(SparkJoinQueryConf conf) {
        JoinQuery query = conf.getQuery();

        Globals.loadTableInfo(query.getTable(), conf.getWorkingDir(),
                HDFSUtils.getFSByHadoopHome(conf.getHadoopHome()));

        TableInfo tableInfo = Globals.getTableInfo(query.getTable());
        key = new RawIndexKey(tableInfo.delimiter);
        opt = new JoinOptimizer(conf);

        opt.loadIndex(tableInfo);
        opt.loadQueries(tableInfo);
    }

    public JoinRobustTree getIndex() {
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
    public PartitionSplit[] getPartitionSplits(JoinQuery q, boolean justAccess) {
        if (justAccess) {
            return opt.buildAccessPlan(q);
        } else {
            return opt.buildPlan(q);
        }
    }

}
