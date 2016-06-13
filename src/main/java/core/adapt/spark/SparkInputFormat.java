package core.adapt.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import core.adapt.AccessMethod;
import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.iterator.IteratorRecord;
import core.adapt.iterator.PartitionIterator;
import core.adapt.iterator.RepartitionIterator;
import core.utils.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.util.*;

public class SparkInputFormat extends
        FileInputFormat<LongWritable, IteratorRecord> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

    SparkQueryConf queryConf;

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> finalSplits = new ArrayList<InputSplit>();
        queryConf = new SparkQueryConf(job.getConfiguration());


        AccessMethod am = new AccessMethod();
        am.init(queryConf);

        HPInput hpInput = new HPInput();
        hpInput.initialize(listStatus(job), am);

        // get the splits based on the query configuration
        PartitionSplit[] splits;
        if (queryConf.getFullScan())
            splits = hpInput.getFullScan(queryConf.getQuery());
        else
            splits = hpInput.getIndexScan(queryConf.getJustAccess(),
                    queryConf.getQuery());

        splits = resizeSplits(splits, hpInput.getPartitionIdSizeMap(),
                queryConf.getMaxSplitSize(), queryConf.getMinSplitSize());
        System.out.println("Number of splits= " + splits.length);

        for (PartitionSplit split : splits) {
            System.out.println("SPLIT: "
                    + split.getIterator().getClass().getName() + " buckets: "
                    + Arrays.toString(split.getPartitions()));
        }

        // create the InputSplit (HDFS object) from the PartitionSplit (internal
        // hyper partitioning object)
        for (PartitionSplit split : splits) {
            PartitionIterator itr = split.getIterator();
            // hack to set the zookeeper hosts
            if (itr instanceof RepartitionIterator) {
                ((RepartitionIterator) itr).setZookeeper(queryConf
                        .getZookeeperHosts());
            }

            SparkFileSplit thissplit = new SparkFileSplit(
                    hpInput.getPaths(split.getPartitions()),
                    hpInput.getLengths(split.getPartitions()), itr);
            finalSplits.add(thissplit);
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES,
                hpInput.getNumPartitions());
        LOG.debug("Total # of splits: " + finalSplits.size());
        System.out.println("done with getting splits");

        return finalSplits;
    }

    private long getPartitionSplitSize(PartitionSplit split,
                                       Map<Integer, Long> partitionIdSizeMap) {
        if (partitionIdSizeMap == null) {
            System.err.println("partition size map is null");
            System.exit(0);
        }
        long size = 0;
        for (int pid : split.getPartitions()) {
            if (partitionIdSizeMap.containsKey(pid))
                size += partitionIdSizeMap.get(pid);
            else
                System.err.println("partitoion size not found: " + pid);
        }
        return size;
    }

    // private Map<Integer,Long> getPartitionIdMap(PartitionSplit[] splits,
    // Multimap<Integer,FileStatus> partitionIdFileMap){
    // Map<Integer,Long> partitionIdSizeMap = Maps.newHashMap();
    //
    // for(PartitionSplit split: splits){
    // int[] partitionIds = split.getPartitions();
    //
    // for(int pid: partitionIds){
    // long size = 0;
    // for(FileStatus fs: partitionIdFileMap.get(pid))
    // size += fs.getLen();
    // size = size / 1024 / 1024;
    // //System.out.println("partition="+pid+", size="+size);
    // if(partitionIdSizeMap.containsKey(pid))
    // partitionIdSizeMap.put(pid, partitionIdSizeMap.get(pid) + size);
    // else
    // partitionIdSizeMap.put(pid, size);
    // }
    //
    // }
    // return partitionIdSizeMap;
    // }

    /**
     * The goal of this method is to check the size of each and break large
     * splits into multiple smaller splits.
     * <p/>
     * The maximum split size is read from the configuration and it depends on
     * the size of each machine.
     *
     * @param initialSplits
     * @return
     */
    public PartitionSplit[] resizeSplits(PartitionSplit[] initialSplits,
                                         Map<Integer, Long> partitionSizes, long maxSplitSize,
                                         long minSplitSize) {
        List<PartitionSplit> resizedSplits = Lists.newArrayList();
        ArrayListMultimap<String, PartitionSplit> smallSplits = ArrayListMultimap
                .create();

        // For statistics count the size of data per iterator.
        Map<String, Long> accessSizes = new HashMap<String, Long>();

        for (PartitionSplit split : initialSplits) {
            long splitSize = getPartitionSplitSize(split, partitionSizes);

            // Add to statistics.
            String iterName = split.getIterator().getClass().getName();
            Long existingCount = accessSizes.containsKey(iterName) ?
                    accessSizes.get(iterName) : (long) 0;
            accessSizes.put(iterName, existingCount + splitSize);

            if (splitSize > maxSplitSize) {
                // create smaller splits
                resizedSplits.addAll(createSmaller(split, partitionSizes,
                        maxSplitSize));
            } else if (splitSize < minSplitSize) {
                // create larger splits
                smallSplits.put(split.getIterator().getClass().getName(), split);
            } else {
                // just accept as it is
                PartitionIterator itr = split.getIterator();
                resizedSplits.add(new PartitionSplit(split.getPartitions(), itr));
            }
        }

        // Print statistics.
        System.out.println("INFO: Access Sizes");
        for (Map.Entry<String, Long> entry : accessSizes.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

        for (String key : smallSplits.keySet())
            resizedSplits.addAll(createLarger(smallSplits.get(key),
                    partitionSizes, maxSplitSize));

        return resizedSplits.toArray(new PartitionSplit[resizedSplits.size()]);
    }

    private List<PartitionSplit> createLarger(List<PartitionSplit> splits,
                                              Map<Integer, Long> partitionSizes, long maxSplitSize) {
        List<PartitionSplit> largerSplits = Lists.newArrayList();

        Multimap<Integer, Integer> largerSplitPartitionIds = ArrayListMultimap
                .create();
        long currentSize = 0;
        int largerSplitId = 0;

        for (PartitionSplit split : splits) {
            for (Integer p : split.getPartitions()) {
                long pSize = partitionSizes.containsKey(p) ? partitionSizes
                        .get(p) : 0;
                if (currentSize + pSize > maxSplitSize) {
                    largerSplitId++;
                    currentSize = 0;
                }
                currentSize += pSize;
                largerSplitPartitionIds.put(largerSplitId, p);
            }
        }

        for (Integer k : largerSplitPartitionIds.keySet()) {
            PartitionIterator itr = splits.get(0).getIterator();
            largerSplits.add(new PartitionSplit(Ints
                    .toArray(largerSplitPartitionIds.get(k)), itr));
        }

        return largerSplits;
    }

    private List<PartitionSplit> createSmaller(PartitionSplit split,
                                               Map<Integer, Long> partitionSizes, long maxSplitSize) {
        List<PartitionSplit> smallerSplits = Lists.newArrayList();

        int[] partitions = split.getPartitions();
        long currentSize = 0;
        Multimap<Integer, Integer> splitPartitionIds = ArrayListMultimap
                .create();
        int splitId = 0;

        for (int i = 0; i < partitions.length; i++) {
            long pSize = partitionSizes.containsKey(partitions[i]) ? partitionSizes
                    .get(partitions[i]) : 0;
            if (currentSize + pSize > maxSplitSize) {
                splitId++;
                currentSize = 0;
            }
            currentSize += pSize;
            splitPartitionIds.put(splitId, partitions[i]);
        }

        for (Integer k : splitPartitionIds.keySet()) {
            PartitionIterator itr = split.getIterator();
            smallerSplits.add(new PartitionSplit(Ints.toArray(splitPartitionIds
                    .get(k)), itr));
        }

        return smallerSplits;
    }

    public PartitionSplit[] resizeSplits(PartitionSplit[] initialSplits,
                                         Map<Integer, Long> partitionSizes, int maxSplitSize) {
        List<PartitionSplit> resizedSplits = Lists.newArrayList();

        for (PartitionSplit split : initialSplits) {
            int[] partitions = split.getPartitions();
            // if(partitions.length > maxSplitSize){
            if (getPartitionSplitSize(split, partitionSizes) > maxSplitSize) {
                // need to split the partition into smaller ones
                int from = 0;
                long currentSize = 0;
                for (int i = 0; i < partitions.length; i++) {
                    long pSize = partitionSizes.get(partitions[i]);
                    if (currentSize > 0 && currentSize + pSize > maxSplitSize) {
                        int[] subPartitions = Arrays.copyOfRange(partitions,
                                from, i);

                        PartitionIterator itr = split.getIterator();
                        resizedSplits.add(new PartitionSplit(subPartitions, itr));

                        from = i;
                        currentSize = 0;
                    }
                    currentSize += pSize;
                }

                if (from < partitions.length - 1) { // the last split
                    int[] subPartitions = Arrays.copyOfRange(partitions, from,
                            partitions.length);
                    PartitionIterator itr = split.getIterator();
                    resizedSplits.add(new PartitionSplit(subPartitions, itr));
                }
            } else {
                PartitionIterator itr = split.getIterator();
                resizedSplits.add(new PartitionSplit(split.getPartitions(), itr));
                resizedSplits.add(split);
            }
        }

        return resizedSplits.toArray(new PartitionSplit[resizedSplits.size()]);
    }

    public PartitionSplit[] combineSplits(PartitionSplit[] initialSplits,
                                          int minSplitSize, int maxSplitSize) {
        List<PartitionSplit> resizedSplits = Lists.newArrayList();
        List<PartitionSplit> smallSplit = Lists.newArrayList();
        int smallSplitSize = 0;

        for (PartitionSplit split : initialSplits) {
            int[] partitions = split.getPartitions();
            if (smallSplitSize + partitions.length > maxSplitSize) {
                int[] mergedIds = new int[0];
                for (PartitionSplit s : smallSplit) {
                    mergedIds = Ints.concat(mergedIds, s.getPartitions());
                }
                PartitionSplit newSplit = new PartitionSplit(mergedIds,
                        smallSplit.get(0).getIterator());
                resizedSplits.add(newSplit);
                smallSplit = Lists.newArrayList();
                smallSplitSize = 0;
            }

            if (split.getIterator().getClass() == RepartitionIterator.class
                    && partitions.length <= minSplitSize) {
                smallSplit.add(split);
                smallSplitSize += partitions.length;
            } else {
                resizedSplits.add(split);
            }

        }

        if (smallSplit.size() > 0) {
            int[] mergedIds = new int[0];
            for (PartitionSplit s : smallSplit) {
                mergedIds = Ints.concat(mergedIds, s.getPartitions());
            }
            PartitionSplit newSplit = new PartitionSplit(mergedIds, smallSplit
                    .get(0).getIterator());
            resizedSplits.add(newSplit);
        }

        return resizedSplits.toArray(new PartitionSplit[resizedSplits.size()]);
    }

    @Override
    public RecordReader<LongWritable, IteratorRecord> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new SparkRecordReader();
    }

    public static class SparkFileSplit extends CombineFileSplit implements
            Serializable {
        private static final long serialVersionUID = 1L;

        private PartitionIterator iterator;

        public SparkFileSplit() {
        }

        public SparkFileSplit(Path[] files, long[] lengths,
                              PartitionIterator iterator) {
            super(files, lengths);
            this.iterator = iterator;
        }

        public SparkFileSplit(Path[] files, long[] start, long[] lengths,
                              String[] locations, PartitionIterator iterator) {
            super(files, start, lengths, locations);
            this.iterator = iterator;
        }

        public static SparkFileSplit read(DataInput in) throws IOException {
            SparkFileSplit s = new SparkFileSplit();
            s.readFields(in);
            return s;
        }

        public PartitionIterator getIterator() {
            return this.iterator;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);
            Text.writeString(out, iterator.getClass().getName());
            iterator.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);
            iterator = (PartitionIterator) ReflectionUtils.getInstance(Text
                    .readString(in));
            iterator.readFields(in);
        }
    }
}
