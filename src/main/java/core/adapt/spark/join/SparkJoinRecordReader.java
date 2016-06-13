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
import core.adapt.spark.join.SparkJoinInputFormat.SparkJoinFileSplit;
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
 * Created by ylu on 12/2/15.
 */


public class SparkJoinRecordReader extends
        RecordReader<LongWritable, Text> {

    protected Configuration conf;
    protected SparkQueryConf queryConf;
    protected SparkJoinFileSplit sparkSplit;
    protected PartitionIterator iter1;
    protected PartitionIterator iter2, pf_iter, jr_iter;
    protected int[] types;
    protected JoinQuery dataset1_joinquery, dataset2_joinquery;
    protected Query dataset2_query;
    ArrayListMultimap<Long, byte[]> hashTable;
    Iterator<byte[]> firstRecords;
    byte[] secondRecord;
    LongWritable key;
    Text value;
    boolean hasNext;
    CuratorFramework client;
    private int currentFile;
    private int numFilesinDataset1;
    private int tupleCountInTable1, tupleCountInTable2; // test only
    private String dataset1, dataset2;
    private int join_attr1, join_attr2;
    private int partitionKey;
    private String delimiter, splitter;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        conf = context.getConfiguration();
        queryConf = new SparkQueryConf(conf);

        client = CuratorUtils.createAndStartClient(conf
                .get(SparkQueryConf.ZOOKEEPER_HOSTS));

        sparkSplit = (SparkJoinFileSplit) split;

        dataset1_joinquery = new JoinQuery(conf.get("DATASET1_QUERY"));
        dataset2_joinquery = new JoinQuery(conf.get("DATASET2_QUERY"));

        dataset2_query = dataset2_joinquery.castToQuery();

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


        iter1 = sparkSplit.getIterator();

        pf_iter = new PostFilterIterator(dataset2_query);
        jr_iter = new JoinRepartitionIterator(dataset2_query);
        ((JoinRepartitionIterator) jr_iter).setZookeeper(queryConf.getZookeeperHosts());


        types = sparkSplit.getTypes();

        key = new LongWritable();
        value = new Text();

        //System.out.println("Path and types >>>>>>>>>>>>>>>>");

        Path[] paths = sparkSplit.getPaths();
        /*
        for(int i = 0; i < paths.length; i ++) {
            System.out.println(paths[i]);
        }

        for(int i = 0; i < types.length; i ++){
            System.out.println(types[i]);
        }
        */

        // build hashtable

        build_hashtable();


        setPartitionToSecondIterator(sparkSplit.getPath(currentFile));

    }

    void setPartitionToSecondIterator(Path path) {
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Loading " + path.toString() + " currentFile " + currentFile);
        HDFSPartition partition = new HDFSPartition(fs, path.toString(),
                Short.parseShort(conf.get(SparkQueryConf.HDFS_REPLICATION_FACTOR)),
                client);
        partition.loadNext(); // ???


        if (types[currentFile - numFilesinDataset1] == 1) {
            iter2 = pf_iter;
        } else {
            iter2 = jr_iter;
        }

        iter2.setPartition(partition);
    }

    void setPartitionToFirstIterator(Path path) {
        FileSystem fs = null;
        try {
            fs = path.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Loading " + path.toString() + " currentFile " + currentFile);
        HDFSPartition partition = new HDFSPartition(fs, path.toString(),
                Short.parseShort(conf.get(SparkQueryConf.HDFS_REPLICATION_FACTOR)),
                client);
        partition.loadNext(); // ???
        iter1.setPartition(partition);
    }

    private void build_hashtable() {

        for (currentFile = 0; currentFile < sparkSplit.getNumPaths(); currentFile++) {

            //System.out.println("TESTING: " + sparkSplit.getPath(currentFile).toString()  + " does it contain " + dataset2);

            if (sparkSplit.getPath(currentFile).toString().contains(dataset2 + "/")) { // solve the issue that dataset names share the same prefix
                iter1.finish();
                numFilesinDataset1 = currentFile;

                break;
            }

            setPartitionToFirstIterator(sparkSplit.getPath(currentFile));

            while (iter1.hasNext()) {

                tupleCountInTable1++;

                IteratorRecord r = iter1.next();
                byte[] rawBytes = r.getBytes();
                long key = r.getLongAttribute(join_attr1);
                hashTable.put(key, rawBytes);
            }
        }
    }

    private void getNext() {

        if (firstRecords != null && firstRecords.hasNext()) {
            hasNext = true;
        } else {
            while (true) {
                if (iter2.hasNext()) {
                    IteratorRecord r = iter2.next();
                    long key = r.getLongAttribute(join_attr2);

                    tupleCountInTable2++;

                    if (hashTable.containsKey(key)) {
                        firstRecords = hashTable.get(key).iterator();
                        secondRecord = r.getBytes();
                        hasNext = true;

                        break;
                    }
                } else {
                    currentFile++;
                    if (currentFile >= sparkSplit.getNumPaths()) {
                        hasNext = false;
                        break;
                    } else {
                        setPartitionToSecondIterator(sparkSplit.getPath(currentFile));
                    }
                }
            }
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
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) currentFile / sparkSplit.getStartOffsets().length;
    }

    @Override
    public void close() throws IOException {

        pf_iter.finish();
        jr_iter.finish(); // write out partitions

        System.out.println("There are " + tupleCountInTable1 + " tuples from the first input!");
        System.out.println("There are " + tupleCountInTable2 + " tuples from the second input!");
    }
}
