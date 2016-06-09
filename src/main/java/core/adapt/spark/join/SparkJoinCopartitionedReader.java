package core.adapt.spark.join;

import com.google.common.collect.ArrayListMultimap;
import core.adapt.HDFSPartition;
import core.adapt.JoinQuery;
import core.adapt.Query;
import core.adapt.iterator.IteratorRecord;
import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.spark.SparkQueryConf;
import core.adapt.spark.join.SparkJoinCopartitionedInputFormat.SparkJoinCopartitionedFileSplit;
import core.utils.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ylu on 4/25/16.
 */
public class SparkJoinCopartitionedReader extends
        RecordReader<LongWritable, Text> {

    private Configuration conf;
    private SparkQueryConf queryConf;
    private SparkJoinCopartitionedFileSplit sparkSplit;
    private PartitionIterator iter1, iter2;
    private int tupleCountInTable1, tupleCountInTable2;
    private LongWritable key;
    private Text value;

    private String dataset1, dataset2;
    private int join_attr1, join_attr2;
    private int partitionKey;
    private String delimiter, splitter;

    protected JoinQuery dataset1_joinquery, dataset2_joinquery;

    ArrayListMultimap<Long, byte[]> hashTable;

    Iterator<byte[]> firstRecords;
    byte[] secondRecord;


    private boolean hasNext;

    private int currentFile;

    CuratorFramework client;


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        conf = context.getConfiguration();
        queryConf = new SparkQueryConf(conf);

        client = CuratorUtils.createAndStartClient(conf
                .get(SparkQueryConf.ZOOKEEPER_HOSTS));


        sparkSplit = (SparkJoinCopartitionedInputFormat.SparkJoinCopartitionedFileSplit) split;

        dataset1_joinquery = new JoinQuery(conf.get("DATASET1_QUERY"));
        dataset2_joinquery = new JoinQuery(conf.get("DATASET2_QUERY"));

        join_attr1 = dataset1_joinquery.getJoinAttribute();
        join_attr2 = dataset2_joinquery.getJoinAttribute();

        partitionKey = Integer.parseInt(conf.get("PARTITION_KEY"));

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        tupleCountInTable1 = 0;
        tupleCountInTable2 = 0;

        delimiter = conf.get("DELIMITER");
        if (delimiter.equals("|"))
            splitter = "\\|";
        else
            splitter = delimiter;

        hashTable = ArrayListMultimap.create();

        iter1 = sparkSplit.getIterator1();
        iter2 = sparkSplit.getIterator2();

        key = new LongWritable();
        value = new Text();


        build_hashtable();

        setPartitionToSecondIterator(sparkSplit.getPath(1));


    }

    void setPartitionToFirstIterator(Path path) {
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Loading " + path.toString());
        HDFSPartition partition = new HDFSPartition(fs, path.toString(),
                Short.parseShort(conf.get(SparkQueryConf.HDFS_REPLICATION_FACTOR)),
                client);
        partition.loadNext(); // ???
        iter1.setPartition(partition);
    }

    void setPartitionToSecondIterator(Path path) {
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Loading " + path.toString());
        HDFSPartition partition = new HDFSPartition(fs, path.toString(),
                Short.parseShort(conf.get(SparkQueryConf.HDFS_REPLICATION_FACTOR)),
                client);
        partition.loadNext(); // ???
        iter2.setPartition(partition);
    }


    private void build_hashtable() {

        setPartitionToFirstIterator(sparkSplit.getPath(currentFile));

        while (iter1.hasNext()) {

            tupleCountInTable1++;

            IteratorRecord r = iter1.next();
            byte[] rawBytes = r.getBytes();
            long key = r.getLongAttribute(join_attr1);
            hashTable.put(key, rawBytes);
        }

    }

    private void getNext() {

        if (firstRecords != null && firstRecords.hasNext()) {
            hasNext = true;
        } else {
            while (iter2.hasNext()) {

                IteratorRecord r = iter2.next();
                long key = r.getLongAttribute(join_attr2);
                tupleCountInTable2++;
                if (hashTable.containsKey(key)) {
                    firstRecords = hashTable.get(key).iterator();
                    secondRecord = r.getBytes();
                    hasNext = true;
                    return;
                }
            }
            hasNext = false;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        getNext();

        if (hasNext) {
            byte[] firstRecord = firstRecords.next();
            String part1 = new String(firstRecord, 0, firstRecord.length);
            String part2 = new String(secondRecord, 0, secondRecord.length);
            String strValue = part1 + delimiter + part2;
            key.set(Long.parseLong(strValue.split(splitter)[partitionKey]));
            value.set(strValue);
        }

        return hasNext;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        iter1.finish();
        iter2.finish();

        System.out.println("There are " + tupleCountInTable1 + " tuples from the first input!");
        System.out.println("There are " + tupleCountInTable2 + " tuples from the second input!");
    }
}
