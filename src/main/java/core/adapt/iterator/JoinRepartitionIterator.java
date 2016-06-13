package core.adapt.iterator;


import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import core.adapt.HDFSPartition;
import core.adapt.Partition;
import core.adapt.Query;
import core.common.globals.Globals;
import core.common.index.JRNode;
import core.common.index.JoinRobustTree;
import core.utils.HDFSUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ylu on 1/27/16.
 */


/**
 * Repartitions the input partitions and writes it out.
 * Does this by reading the new index. For each tuple, gets its new bucket id.
 * Writes it out the corresponding bucket.
 * Does not delete old partitions
 *
 * @author yil
 */
public class JoinRepartitionIterator extends PartitionIterator {

    /*
    class PartitionStoreThread extends Thread {
        Partition p;
        PartitionStoreThread(Partition p){
            this.p = p;
        }
        public void run() {
            System.out.println("storing partition id " + p.getPartitionId());
            p.store(true);
        }
    }
    */

    protected String zookeeperHosts;
    protected Map<Integer, Partition> newPartitions = new HashMap<Integer, Partition>();
    private JRNode newIndexTree;
    //protected Map<Integer, Partition> oldPartitions = new HashMap<Integer, Partition>();

    public JoinRepartitionIterator() {
    }

    public JoinRepartitionIterator(String iteratorString) {
        try {
            readFields(ByteStreams.newDataInput(iteratorString.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read the fields");
        }
    }

    public JoinRepartitionIterator(Query query) {
        super(query);
    }

    public JoinRepartitionIterator(Query query, JRNode tree) {
        this.query = query;
        this.newIndexTree = tree;
    }

    public static RepartitionIterator read(DataInput in) throws IOException {
        RepartitionIterator it = new RepartitionIterator();
        it.readFields(in);
        return it;
    }

    public void setZookeeper(String zookeeperHosts) {
        this.zookeeperHosts = zookeeperHosts;
    }

    public Query getQuery() {
        return this.query;
    }

    public JRNode getIndexTree() {
        return this.newIndexTree;
    }

    /**
     * Gets a HDFS Partition as input.
     * Loads the new index. PartitionWriter::setPartition does the rest.
     */
    @Override
    public void setPartition(Partition partition) {
        super.setPartition(partition);
        if (newIndexTree == null) {
            String path = FilenameUtils.getPathNoEndSeparator(partition
                    .getPath());

            if (FilenameUtils.getBaseName(path).contains("partitions")
                    || FilenameUtils.getBaseName(path).contains("repartition")) { // hack
                path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
            }

            if (FilenameUtils.getBaseName(path).contains("data")) { // hack
                path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
            }

            // Initialize RobustTree.
            byte[] indexBytes = HDFSUtils.readFile(
                    ((HDFSPartition) partition).getFS(), path + "/index");
            JoinRobustTree tree = new JoinRobustTree(Globals.getTableInfo(query.getTable()));
            tree.unmarshall(indexBytes);
            newIndexTree = tree.getRoot();
        }
        //oldPartitions.put(partition.getPartitionId(), partition);
    }

    @Override
    protected boolean isRelevant(IteratorRecord record) {
        int id = newIndexTree.getBucketId(record);

        Partition p;
        if (newPartitions.containsKey(id)) {
            p = newPartitions.get(id);
        } else {
            p = partition.clone();
            p.setPartitionId(id);
            newPartitions.put(id, p);
        }

        p.write(record.getBytes(), 0, record.getBytes().length);

        if (p.size() > 2 * 1024 * 1024) { // 2mb
            p.store(true);
            newPartitions.remove(id);
        }

        return query.qualifies(record);
    }

    @Override
    public void finish() {
        if (zookeeperHosts != null) {

            for (Partition p : newPartitions.values()) {
                System.out.println("storing partition id " + p.getPartitionId());
                p.store(true);
            }

            /*
            for (Partition p : oldPartitions.values()) {
                System.out.println("dropping old partition id "
                        + p.getPartitionId());
                p.drop();
            }
            */

            /*

            ExecutorService pool = Executors.newFixedThreadPool(20);

            for (Partition p : newPartitions.values()) {
                pool.submit( new PartitionStoreThread(p));
            }

            pool.shutdown();
            */

            //oldPartitions = Maps.newHashMap();
            newPartitions = Maps.newHashMap();
        } else {
            System.out.println("INFO: Zookeeper Hosts NULL");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        query.write(out);
        Text.writeString(out, zookeeperHosts);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String predicateString = Text.readString(in);
        query = new Query(predicateString);
        zookeeperHosts = Text.readString(in);
    }
}
