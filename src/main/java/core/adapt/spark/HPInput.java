package core.adapt.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import core.adapt.AccessMethod;
import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.Query;
import core.adapt.iterator.PostFilterIterator;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;

public class HPInput {

    protected AccessMethod am;
    protected ArrayListMultimap<Integer, FileStatus> partitionIdFileMap;
    protected Map<Integer, Long> partitionIdSizeMap;

    public void initialize(List<FileStatus> files, AccessMethod am) {
        this.am = am;
        partitionIdFileMap = ArrayListMultimap.create();
        partitionIdSizeMap = Maps.newHashMap();
        for (FileStatus file : files) {
            try {
                int id = Integer.parseInt(FilenameUtils.getName(file.getPath()
                        .toString()));
                partitionIdFileMap.put(id, file);
                long currentSize = partitionIdSizeMap.containsKey(id) ? partitionIdSizeMap
                        .get(id) : 0;
                partitionIdSizeMap.put(id, currentSize + file.getLen());
            } catch (NumberFormatException e) {
            }
        }
    }

    public PartitionSplit[] getFullScan(Query q) {
        return new PartitionSplit[]{new PartitionSplit(
                Ints.toArray(partitionIdFileMap.keySet()),
                new PostFilterIterator(q))};
    }

    public PartitionSplit[] getIndexScan(boolean justAccess,
                                         Query q) {
        return am.getPartitionSplits(q, justAccess);
    }

    // utility methods

    public Path[] getPaths(int[] partitionIds) {
        List<Path> splitFiles = Lists.newArrayList();
        for (int i = 0; i < partitionIds.length; i++)
            for (FileStatus fs : partitionIdFileMap.get(partitionIds[i]))
                splitFiles.add(fs.getPath());
        Path[] splitFilesArr = new Path[splitFiles.size()];
        for (int i = 0; i < splitFilesArr.length; i++)
            splitFilesArr[i] = splitFiles.get(i);

        return splitFilesArr;
    }

    public Path[] getPaths(PartitionSplit[] splits) {
        int totalLength = 0;
        for (PartitionSplit split : splits) {
            totalLength += split.getPartitions().length;
        }
        int[] partitions = new int[totalLength];
        int currentIndex = 0;
        for (PartitionSplit split : splits) {
            for (int id : split.getPartitions()) {
                partitions[currentIndex] = id;
                currentIndex++;
            }
        }
        return getPaths(partitions);
    }

    public long[] getLengths(int[] partitionIds) {
        List<Long> lengths = Lists.newArrayList();
        for (int i = 0; i < partitionIds.length; i++)
            for (FileStatus fs : partitionIdFileMap.get(partitionIds[i]))
                lengths.add(fs.getLen());

        long[] lengthsArr = new long[lengths.size()];
        for (int i = 0; i < lengthsArr.length; i++)
            lengthsArr[i] = lengths.get(i);

        return lengthsArr;
    }

    public long[] getLengths(PartitionSplit[] splits) {
        int totalLength = 0;
        for (PartitionSplit split : splits) {
            totalLength += split.getPartitions().length;
        }
        int[] partitions = new int[totalLength];
        int currentIndex = 0;
        for (PartitionSplit split : splits) {
            for (int id : split.getPartitions()) {
                partitions[currentIndex] = id;
                currentIndex++;
            }
        }
        return getLengths(partitions);
    }

    public long getTotalSize(PartitionSplit[] splits) {
        long length = 0;
        for (PartitionSplit split : splits) {
            for (int id : split.getPartitions()) {
                for (FileStatus fs : partitionIdFileMap.get(id)) {
                    length += fs.getLen();
                }
            }
        }
        return length;
    }

    public int getNumPartitions() {
        return partitionIdSizeMap.size();
    }

    public Map<Integer, Long> getPartitionIdSizeMap() {
        return partitionIdSizeMap;
    }
}
