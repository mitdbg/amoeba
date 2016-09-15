package core.common.index;

import core.common.key.ParsedTupleList;
import core.common.key.RawIndexKey;
import core.utils.Range;
import core.utils.TypeUtils;

import java.io.Serializable;
import java.util.HashMap;

/**
 * An API for implementing multi-dimensional index, e.g. R-Tree, K-d Tree, etc.
 */
public interface MDIndex extends Serializable {

    MDIndex clone() throws CloneNotSupportedException;

    int getMaxBuckets();

	/*
     *
	 * The Build phase of the index
	 */

    void setMaxBuckets(int maxBuckets);

    /*
     *
     * The Probe phase of the index
     */
    void initProbe();

    void initProbe(int joinAttribute);

    /**
     * Get the bucket id, for a given key, from an existing index.
     *
     * @param key
     * @return
     */
    Integer getBucketId(RawIndexKey key);

    /**
     * Serialize the index into a byte array.
     *
     * @return serialized index.
     */
    byte[] marshall();

    /**
     * Deserialize the index from a byte array.
     *
     * @param bytes
     */
    void unmarshall(byte[] bytes);

    /*
     * Placeholder class for the index leaves.
     */
    final class Bucket implements Serializable {
        public static int maxBucketId = 0;
        private static HashMap<Integer, Double> counter = new HashMap<Integer, Double>();
        /* Actual Values */
        int bucketId;
        ParsedTupleList sample;
        /* Estimates */
        private double estimatedTuples = 0;

        public Bucket() {
            bucketId = maxBucketId;
            maxBucketId += 1;
        }

        public Bucket(int id) {
            bucketId = id;
            maxBucketId = Math.max(bucketId + 1, maxBucketId);
        }

        public static double getEstimatedNumTuples(int bucketId) {
            Double val = counter.get(bucketId);
            return val;
        }

        public double getEstimatedNumTuples() {
            // TODO: This call used when restoring a replaced tree in Optimizer.
            // Can't use the assert below.
            // Assert.assertNotEquals(estimatedTuples, 0.0);
            return estimatedTuples;
        }

        public void setEstimatedNumTuples(double num) {
            estimatedTuples = num;
            counter.put(bucketId, num);
        }

        public int getBucketId() {
            return bucketId;
        }

        public void updateId() {
            this.bucketId = maxBucketId;
            maxBucketId += 1;
            estimatedTuples = 0;
        }

        public ParsedTupleList getSample() {
            return sample;
        }

        public void setSample(ParsedTupleList sample) {
            this.sample = sample;
        }
    }

    /**
     * Created by qui on 7/9/15.
     */
    class BucketInfo extends Range implements Serializable {

        private int id;

        public BucketInfo(int id, TypeUtils.TYPE type, Object low, Object high) {
            super(type, low, high);
            this.id = id;
        }

        public BucketInfo(TypeUtils.TYPE type, Object low, Object high) {
            this(0, type, low, high);
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @Override
        public BucketInfo clone() {
            return new BucketInfo(id, getType(), getLow(), getHigh());
        }
    }
}
