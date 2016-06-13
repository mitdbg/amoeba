package core.adapt.spark.join;

import core.adapt.JoinQuery;
import core.adapt.iterator.JoinRepartitionIterator;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.spark.SparkQueryConf;
import core.utils.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
 * Created by ylu on 12/2/15.
 */


public class SparkJoinInputFormat extends
        FileInputFormat<LongWritable, Text> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

    private Configuration conf;
    private SparkQueryConf queryConf;

    private String dataset1, dataset2;
    private JoinQuery dataset1_query, dataset2_query;

    private SparkJoinFileSplit formSplits(Path[] paths1, long[] lengths1, Path[] paths2, long[] lengths2, PartitionIterator iter, int[] types) {
        int totalLength = paths1.length + paths2.length;
        Path[] paths = new Path[totalLength];
        long[] lengths = new long[totalLength];
        for (int i = 0; i < totalLength; i++) {
            if (i < paths1.length) {
                paths[i] = paths1[i];
                lengths[i] = lengths1[i];
            } else {
                paths[i] = paths2[i - paths1.length];
                lengths[i] = lengths2[i - paths1.length];
            }
        }
        return new SparkJoinFileSplit(paths, lengths, iter, types);
    }

    void fill_data1(String[] data, String dataset, Path[] paths, long[] lens) {
        int size = data.length;

        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();

        for (int i = 0, j = 0; i < size; i += 2, j++) {
            int id = Integer.parseInt(data[i]);
            long length = Long.parseLong(data[i + 1]);

            paths[j] = new Path(hdfsDefaultName + workingDir + "/" + dataset + "/data/" + id);
            lens[j] = length;
        }
    }

    void fill_data2(String[] data, String dataset, Path[] paths, long[] lens, int[] types) {
        int size = data.length;

        String hdfsDefaultName = conf.get("fs.default.name");
        String workingDir = queryConf.getWorkingDir();

        for (int i = 0, j = 0; i < size; i += 3, j++) {
            int id = Integer.parseInt(data[i]);
            long length = Long.parseLong(data[i + 1]);
            int type = Integer.parseInt(data[i + 2]);
            paths[j] = new Path(hdfsDefaultName + workingDir + "/" + dataset + "/data/" + id);
            lens[j] = length;
            types[j] = type;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        System.out.println("INFO: in getSplits");

        List<InputSplit> finalSplits = new ArrayList<InputSplit>();
        conf = job.getConfiguration();
        queryConf = new SparkQueryConf(conf);

        dataset1 = conf.get("DATASET1");
        dataset2 = conf.get("DATASET2");

        dataset1_query = new JoinQuery(conf.get("DATASET1_QUERY"));
        dataset2_query = new JoinQuery(conf.get("DATASET2_QUERY"));

        String dataset_info = conf.get("DATASETINFO");

        if (dataset_info.length() == 0) {
            return finalSplits;
        }

        // iter1_type, id1:len1:id2:len2:... , id1:len1:iter2_type:id2:len2:iter2_type... ;...

        PartitionIterator iter;

        String[] splits = dataset_info.split(";");
        for (int i = 0; i < splits.length; i++) {
            String[] subsplits = splits[i].split(",");
            int iter_type = Integer.parseInt(subsplits[0]);

            if (iter_type == 1) {
                iter = new PostFilterIterator(dataset1_query.castToQuery());

            } else {
                iter = new JoinRepartitionIterator(dataset1_query.castToQuery());
                ((JoinRepartitionIterator) iter).setZookeeper(queryConf.getZookeeperHosts());
            }

            String[] data1 = subsplits[1].split(":");
            String[] data2 = subsplits[2].split(":");

            int size1 = data1.length / 2, size2 = data2.length / 3;

            Path[] paths1 = new Path[size1];
            long[] lens1 = new long[size1];

            Path[] paths2 = new Path[size2];
            long[] lens2 = new long[size2];
            int[] types2 = new int[size2];

            fill_data1(data1, dataset1, paths1, lens1);
            fill_data2(data2, dataset2, paths2, lens2, types2);

            SparkJoinFileSplit thissplit = formSplits(paths1, lens1, paths2, lens2, iter, types2);
            finalSplits.add(thissplit);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, finalSplits.size());
        LOG.debug("Total # of splits: " + finalSplits.size());
        System.out.println("done with getting splits");

        return finalSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new SparkJoinRecordReader();
    }

    public static class SparkJoinFileSplit extends CombineFileSplit implements
            Serializable {
        private static final long serialVersionUID = 1L;

        private PartitionIterator iter;
        private int[] types;

        public SparkJoinFileSplit() {
        }

        public SparkJoinFileSplit(Path[] files, long[] lengths, PartitionIterator iter, int[] types) {
            super(files, lengths);
            this.iter = iter;
            this.types = types;
        }

        public static SparkJoinFileSplit read(DataInput in) throws IOException {
            SparkJoinFileSplit s = new SparkJoinFileSplit();
            s.readFields(in);
            return s;
        }

        public PartitionIterator getIterator() {
            return iter;
        }

        public int[] getTypes() {
            return types;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);

            Text.writeString(out, iter.getClass().getName());
            iter.write(out);

            out.writeInt(types.length);
            for (int i = 0; i < types.length; i++) {
                out.writeInt(types[i]);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);

            String type = Text.readString(in);
            iter = (PartitionIterator) ReflectionUtils.getInstance(type);
            iter.readFields(in);

            int length = in.readInt();
            types = new int[length];
            for (int i = 0; i < length; i++) {
                types[i] = in.readInt();
            }
        }
    }
}