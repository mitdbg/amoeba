package core.adapt.spark.join;

import core.adapt.HDFSPartition;
import core.adapt.JoinQuery;
import core.adapt.Query;
import core.adapt.iterator.IteratorRecord;
import core.adapt.iterator.PartitionIterator;
import core.adapt.spark.SparkQueryConf;
import core.adapt.spark.join.SparkScanInputFormat.SparkScanFileSplit;
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

/**
 * Created by ylu on 1/6/16.
 */
public class SparkScanRecordReader extends
        RecordReader<LongWritable, Text> {

    private Configuration conf;
    private SparkQueryConf queryConf;
    private SparkScanFileSplit sparkSplit;
    private CuratorFramework client;
    private PartitionIterator iter;
    private int join_attr;
    private int tupleCountInTable; // test only
    private int[] types;
    private LongWritable key;
    private Text value;
    private Query query;

    private boolean hasNext;

    private int currentFile;

    void setPartitionToIterator(Path path) {
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

        iter.setPartition(partition);

    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        conf = context.getConfiguration();
        queryConf = new SparkQueryConf(conf);
        client = CuratorUtils.createAndStartClient(conf
                .get(SparkQueryConf.ZOOKEEPER_HOSTS));

        sparkSplit = (SparkScanFileSplit) split;

        JoinQuery jq = new JoinQuery(conf.get("DATASET_QUERY"));
        ;

        query = jq.castToQuery();
        join_attr = jq.getJoinAttribute();


        key = new LongWritable(0);
        value = new Text();

        tupleCountInTable = 0;

        iter = sparkSplit.getIterator();

        currentFile = 0;
        setPartitionToIterator(sparkSplit.getPath(currentFile));

    }

    private void getNext() {

        while (true) {
            if (iter.hasNext()) {
                IteratorRecord r = iter.next();

                key.set(r.getLongAttribute(join_attr));
                value.set(r.getBytes());

                tupleCountInTable++;
                hasNext = true;
                break;

            } else {
                currentFile++;
                if (currentFile >= sparkSplit.getNumPaths()) {
                    hasNext = false;
                    break;
                } else {
                    setPartitionToIterator(sparkSplit.getPath(currentFile));
                }
            }
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        getNext();
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
        iter.finish(); // this method could even be called earlier in case
        // the entire split does not fit in main-memory
        // counter.close();
        // locker.cleanup();
        System.out.println("There are " + tupleCountInTable + " tuples the input!");
    }
}
