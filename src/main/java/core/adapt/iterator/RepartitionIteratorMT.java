package core.adapt.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import core.utils.Pair;
import org.apache.hadoop.io.Text;

import com.google.common.io.ByteStreams;

import core.adapt.Partition;
import core.adapt.Query;
import core.common.index.RNode;

/**
 * Repartitions the input partitions and writes it out.
 * Does this by reading the new index. For each tuple, gets its new bucket id.
 * Writes it out the corresponding bucket.
 * @author anil
 *
 */
public class RepartitionIteratorMT extends PartitionIteratorMT {
    public class BufferManager {
        Map<Integer, Pair<Lock, Partition>> partitions = new ConcurrentHashMap<>();
        Partition basePartition;

        String zookeeperHosts;

        public BufferManager(String zh, Partition basePartition) {
            this.zookeeperHosts = zh;
            this.basePartition = partition;
        }

        public void addRecord(int id, IteratorRecord record) {
            Pair<Lock, Partition> p;
            if (partitions.containsKey(id)) {
                p = partitions.get(id);
            } else {
                Partition partition = basePartition.clone();
                partition.setPartitionId(id);
                Lock l = new ReentrantLock();
                p = new Pair(l, partition);
                partitions.put(id, p);
            }

            p.first.lock();
            try {
                p.second.write(record.getBytes(), record.getOffset(), record.getLength());
            } finally {
                p.first.unlock();
            }
        }

        public void finish() {
            for (Pair<Lock, Partition> p : partitions.values()) {
                System.out.println("storing partition id " + p.second.getPartitionId());
                p.second.store(true);
            }
        }
    }

    private RNode newIndexTree;
    protected String zookeeperHosts;
    private BufferManager bufferManager;

    public RepartitionIteratorMT() {
    }

    public RepartitionIteratorMT(String iteratorString) {
        try {
            readFields(ByteStreams.newDataInput(iteratorString.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read the fields");
        }
    }

    public RepartitionIteratorMT(Query q, Partition p, BufferManager bm, RNode tree, String zookeeperHosts) {
        super(q, p);
        this.bufferManager = bm;
        this.newIndexTree = tree;
        this.zookeeperHosts = zookeeperHosts;
    }

    public Query getQuery() {
        return this.query;
    }

    @Override
    protected boolean isRelevant(IteratorRecord record) {
        int id = newIndexTree.getBucketId(record);
        bufferManager.addRecord(id, record);
        return query.qualifies(record);
    }

    @Override
    public void finish() {
        System.out.println("dropping old partition id "
                + partition.getPartitionId());
        partition.drop();
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

    public static RepartitionIterator read(DataInput in) throws IOException {
        RepartitionIterator it = new RepartitionIterator();
        it.readFields(in);
        return it;
    }
}
