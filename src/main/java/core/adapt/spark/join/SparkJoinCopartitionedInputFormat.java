package core.adapt.spark.join;

import core.adapt.JoinQuery;
import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.spark.SparkQueryConf;
import core.utils.HDFSUtils;
import core.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ylu on 4/25/16.
 */
public class SparkJoinCopartitionedInputFormat extends
        FileInputFormat<LongWritable, Text> implements Serializable {


    public static class SparkJoinCopartitionedFileSplit extends CombineFileSplit implements
            Serializable {
        private static final long serialVersionUID = 1L;

        private PartitionIterator iter1, iter2;
        public SparkJoinCopartitionedFileSplit() {

        }

        public SparkJoinCopartitionedFileSplit(Path[] files, long[] lengths, PartitionIterator iter1, PartitionIterator iter2) {
            super(files, lengths);
            this.iter1 = iter1;
            this.iter2 = iter2;
        }

        public PartitionIterator getIterator1() {
            return iter1;
        }
        public PartitionIterator getIterator2() {
            return iter2;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);

            Text.writeString(out, iter1.getClass().getName());
            iter1.write(out);
            Text.writeString(out, iter2.getClass().getName());
            iter2.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);

            String type = Text.readString(in);
            iter1 = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter1.readFields(in);
            type = Text.readString(in);
            iter2 = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter2.readFields(in);

        }

        public static SparkJoinCopartitionedFileSplit read(DataInput in) throws IOException {
            SparkJoinCopartitionedFileSplit s = new SparkJoinCopartitionedFileSplit();
            s.readFields(in);
            return s;
        }
    }


    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("In SparkJoinCopartitionedInputFormat getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();

        Configuration conf = job.getConfiguration();

        SparkQueryConf queryConf = new SparkQueryConf(conf);


        JoinQuery jq1 =  new JoinQuery(conf.get("DATASET1_QUERY"));
        JoinQuery jq2 =  new JoinQuery(conf.get("DATASET2_QUERY"));;

        String dataset1 = conf.get("DATASET1");
        String dataset2 = conf.get("DATASET2");

        int partition_num = Integer.parseInt(conf.get("PARTITION_NUM"));

        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();

        PartitionIterator iter1 = new PostFilterIterator(jq1.castToQuery());
        PartitionIterator iter2 = new PostFilterIterator(jq2.castToQuery());

        FileSystem fs = HDFSUtils.getFSByHadoopHome(queryConf.getHadoopHome());

        for(int i = 0; i < partition_num; i ++){

            String path1 = hdfsDefaultName + workingDir +  "/" + dataset1 + "/" + i;
            String path2 = hdfsDefaultName + workingDir +  "/" + dataset2 + "/" + i;

            Path[] paths = new Path[2];
            paths[0] = new Path(path1);
            paths[1] = new Path(path2);

            long[] lengths = new long[2];

            lengths[0] = fs.getFileStatus(paths[0]).getLen();
            lengths[1] = fs.getFileStatus(paths[1]).getLen();

            SparkJoinCopartitionedFileSplit split = new SparkJoinCopartitionedFileSplit(paths, lengths, iter1, iter2);
            finalSplits.add(split);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, finalSplits.size());
        System.out.println("done with getting splits");
        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SparkJoinCopartitionedReader();
    }
}
