package core.adapt.spark.join;

/**
 * Created by ylu on 12/3/15.
 */


import com.google.common.primitives.Ints;
import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.JoinQuery;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.iterator.RepartitionIterator;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HPJoinInput {

    protected JoinAccessMethod am;
    protected Map<Integer, FileStatus> partitionIdFileMap;
    protected Map<Integer, Long> partitionIdSizeMap;
    protected boolean MDIndexInput;


    public HPJoinInput(boolean MDIndexInput) {
        this.MDIndexInput = MDIndexInput;
    }

    public void initialize(List<FileStatus> files, JoinAccessMethod am) {
        this.am = am;
        initialize(files);
    }

    public void initialize(List<FileStatus> files) {
        partitionIdFileMap = new HashMap<Integer, FileStatus>();
        partitionIdSizeMap = new HashMap<Integer, Long>();
        for (FileStatus file : files) {
            System.out.println("FILE: " + file.getPath());
            try {
                String fileName = FilenameUtils.getName(file.getPath().toString());
                int id = 0;
                if (MDIndexInput) {
                    id = Integer.parseInt(fileName);
                } else {
                    id = Integer.parseInt(fileName.substring(fileName.indexOf('-') + 1));
                }
                partitionIdFileMap.put(id, file);
                partitionIdSizeMap.put(id, file.getLen());
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
    }

    public PartitionSplit[] getFullScan(JoinQuery q) {
        return new PartitionSplit[]{new PartitionSplit(
                Ints.toArray(partitionIdFileMap.keySet()),
                new PostFilterIterator(q.castToQuery()))};
    }

    public PartitionSplit[] getRepartitionScan(JoinQuery q) {
        return new PartitionSplit[]{new PartitionSplit(
                Ints.toArray(partitionIdFileMap.keySet()),
                new RepartitionIterator(q.castToQuery()))};
    }

    public PartitionSplit[] getIndexScan(boolean justAccess,
                                         JoinQuery q) {
        return am.getPartitionSplits(q, justAccess);
    }

    // utility methods

    public Path[] getPaths(int[] partitionIds) {

        Path[] splitFilesArr = new Path[partitionIds.length];
        for (int i = 0; i < splitFilesArr.length; i++)
            splitFilesArr[i] = partitionIdFileMap.get(partitionIds[i]).getPath();
        return splitFilesArr;
    }


    public long[] getLengths(int[] partitionIds) {
        long[] lengthsArr = new long[partitionIds.length];
        for (int i = 0; i < lengthsArr.length; i++) {
            lengthsArr[i] = partitionIdSizeMap.get(partitionIds[i]);
        }
        return lengthsArr;
    }

    public Map<Integer, Long> getPartitionIdSizeMap() {
        return partitionIdSizeMap;
    }
}
