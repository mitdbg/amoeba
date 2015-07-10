package core.access;

import core.index.MDIndex;
import core.utils.Range;
import core.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qui on 7/9/15.
 */
public class PartitionRange extends Range {

    private List<MDIndex.BucketInfo> buckets;

    public PartitionRange(SchemaUtils.TYPE type, Object low, Object high) {
        super(type, low, high);
        this.buckets = new ArrayList<MDIndex.BucketInfo>();
    }

    public void addBucket(MDIndex.BucketInfo bucketInfo) {
        addBucket(bucketInfo, true);
    }

    public void addBucket(MDIndex.BucketInfo bucketInfo, boolean updateRange) {
        buckets.add(bucketInfo);
        if (updateRange) {
            this.union(bucketInfo);
        }
    }

    public List<MDIndex.BucketInfo> getBuckets() {
        return buckets;
    }
}
