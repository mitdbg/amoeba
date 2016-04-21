package core.adapt.spark.join;

import core.adapt.JoinQuery;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.spark.SparkQueryConf;
import core.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ylu on 1/6/16.
 */

public class SparkScanInputFormat extends FileInputFormat<LongWritable, Text> implements Serializable {

    private String dataset;


    private Configuration conf;
    private SparkQueryConf queryConf;


    public static class SparkScanFileSplit extends CombineFileSplit implements
            Serializable {
        private static final long serialVersionUID = 1L;

        private PartitionIterator iter;

        public SparkScanFileSplit() {
        }

        public SparkScanFileSplit(Path[] files, long[] lengths, PartitionIterator iter) {
            super(files, lengths);
            this.iter = iter;
        }

        public PartitionIterator getIterator() {
            return iter;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);

            Text.writeString(out, iter.getClass().getName());
            iter.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);
            String type = Text.readString(in);
            iter = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter.readFields(in);
        }

        public static SparkScanFileSplit read(DataInput in) throws IOException {
            SparkScanFileSplit s = new SparkScanFileSplit();
            s.readFields(in);
            return s;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("In SparkScanInputFormat getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();

        conf = job.getConfiguration();

        queryConf = new SparkQueryConf(conf);

        String dataset_info = conf.get("DATASETINFO");
        JoinQuery jq =  new JoinQuery(conf.get("DATASET_QUERY"));;

        dataset = conf.get("DATASET");

        if (dataset_info.length() == 0) {
            return finalSplits;
        }


        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();

        PartitionIterator iter;

        String[] splits = dataset_info.split(";");

        for (int i = 0; i < splits.length; i++) {
            String[] subsplits = splits[i].split(",");
            int iter_type = Integer.parseInt(subsplits[0]);

            if (iter_type == 1) {
                iter = new PostFilterIterator(jq.castToQuery());

            } else {
                iter = new JoinRepartitionIterator(jq.castToQuery());
                ((JoinRepartitionIterator) iter).setZookeeper(queryConf.getZookeeperHosts());
            }


            Path[] path = new Path[subsplits.length - 1];
            long[] len = new long[subsplits.length - 1];

            for (int j = 0; j < path.length; j++) {
                String[] ss = subsplits[j + 1].split(":");
                int id = Integer.parseInt(ss[0]);
                long length = Long.parseLong(ss[1]);
                path[j] = new Path(hdfsDefaultName + workingDir + "/" + dataset + "/data/" + id);
                len[j] = length;
            }

            SparkScanFileSplit split = new SparkScanFileSplit(path, len, iter);
            finalSplits.add(split);
        }


        job.getConfiguration().setLong(NUM_INPUT_FILES, finalSplits.size());
        System.out.println("done with getting splits");
        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SparkScanRecordReader();
    }
}
