package core.simulator;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.Query;
import core.adapt.iterator.RepartitionIterator;
import core.adapt.opt.Optimizer;
import core.common.globals.Globals;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;

public class Simulator {
    Optimizer opt;

    ConfUtils cfg;

    String simName;

    String dataset;

    Query[] queries;

    public void setUp(ConfUtils cfg, String simName, String dataset, Query[] queries) {
        this.cfg = cfg;
        this.simName = simName;
        this.dataset = dataset;
        this.queries = queries;

        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        HDFSUtils.deleteFile(fs,
                cfg.getHDFS_WORKING_DIR() + "/" + simName, true);
        HDFSUtils.safeCreateDirectory(fs, cfg.getHDFS_WORKING_DIR() + "/" + simName);

        CuratorFramework client = CuratorUtils.createAndStartClient(
                cfg.getZOOKEEPER_HOSTS());
        CuratorUtils.deleteAll(client, "/", "partition-");
        CuratorUtils.stopClient(client);

        HDFSUtils.copyFile(fs, cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/" + "index",
                cfg.getHDFS_WORKING_DIR() + "/" + simName + "/" + "index",
                cfg.getHDFS_REPLICATION_FACTOR());
        HDFSUtils.copyFile(fs, cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/" + "sample",
                cfg.getHDFS_WORKING_DIR() + "/" + simName + "/" + "sample",
                cfg.getHDFS_REPLICATION_FACTOR());
        HDFSUtils.copyFile(fs, cfg.getHDFS_WORKING_DIR() + "/" + dataset + "/" + "info",
                cfg.getHDFS_WORKING_DIR() + "/" + simName + "/" + "info",
                cfg.getHDFS_REPLICATION_FACTOR());

        Globals.loadTableInfo(simName, cfg.getHDFS_WORKING_DIR(), fs);

        opt = new Optimizer(cfg);
        opt.loadIndex(Globals.getTableInfo(simName));
    }

    public void runOld() {
        for (int i = 0; i < queries.length; i++) {
            Query q = queries[i];
            q.setTable(simName);
            PartitionSplit[] splits = opt.buildPlan(q);

            // Check if there was an index update. If yes, we need
            // to reload the index.
            for (PartitionSplit p : splits) {
                if (p.getIterator().getClass() == RepartitionIterator.class) {
                    System.out.println("INFO: Reloading index ..");
                    opt.loadIndex(Globals.getTableInfo(simName));
                    break;
                }
            }
        }
    }

    /**
     * Runs the queries against the partitioning tree.
     * Does not use the adaptive executor.
     */
    public void runNoAdapt() {
        for (int i = 0; i < queries.length; i++) {
            Query q = queries[i];
            q.setTable(simName);
            PartitionSplit[] splits = opt.buildAccessPlan(q);
        }
    }

    /**
     * Runs the queries against the partitioning tree.
     * Does not use the adaptive executor.
     */
    public void runAdapt() {
        for (int i = 0; i < queries.length; i++) {
            Query q = queries[i];
            q.setTable(simName);
            PartitionSplit[] splits = opt.buildMultiPredicatePlan(q);

            // When using the multi-predicate plan builder, the rt gets dirtied
            // We need to reload for every query.
            opt.loadIndex(Globals.getTableInfo(simName));
        }
    }
}
