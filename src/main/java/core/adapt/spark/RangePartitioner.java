package core.adapt.spark;

import core.utils.RangePartitionerUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.Partitioner;

/**
 * Created by ylu on 12/14/15.
 */

public class RangePartitioner extends Partitioner {

    long[] ranges;

    public RangePartitioner(String ranges) {
        this.ranges = RangePartitionerUtils.getLongCutPoints(ranges);
    }

    @Override
    public int numPartitions() {
        return ranges.length + 1;
    }

    @Override
    public int getPartition(Object key) {
        //hard code, key can only be long

        long longKey = ((LongWritable) key).get();
        for (int i = 0; i < ranges.length; i++) {
            if (longKey <= ranges[i]) {
                return i;
            }
        }

        return ranges.length;
    }
}