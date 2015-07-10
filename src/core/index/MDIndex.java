package core.index;
import core.utils.Range;
import core.utils.SchemaUtils;
import org.apache.curator.framework.CuratorFramework;

import core.index.key.CartilageIndexKeySet;
import core.index.key.MDIndexKey;
import core.utils.CuratorUtils;


/**
 *
 * An API for implementing multi-dimensional index, e.g. R-Tree, K-d Tree, etc.
 *
 */

public interface MDIndex {

	/*
	 * Placeholder class for the index leaves.
	 *
	 */

	public final static class Bucket{
		/* Actual Values */
		int bucketId;
		CartilageIndexKeySet sample;

		/* Estimates */
		public float estimatedTuples;

		public static int maxBucketId = 0;
		public static BucketCounts counters;
		
		private int bucketCount;

		public Bucket() {
			bucketId = maxBucketId;
			bucketCount = -1;
			maxBucketId += 1;
		}

		public Bucket(int id) {
			bucketId = id;
			maxBucketId = Math.max(bucketId+1, maxBucketId);
			bucketCount = -1;
		}

		public int getNumTuples() {
			if(bucketCount == -1)
				bucketCount = counters.getBucketCount(bucketId);
			return bucketCount;
		}

		public int getBucketId() {
			return bucketId;
		}

		public void updateId() {
			this.bucketId = maxBucketId;
			maxBucketId += 1;
			bucketCount = -1;
		}

		public CartilageIndexKeySet getSample() {
			return sample;
		}

		public void setSample(CartilageIndexKeySet sample) {
			this.sample = sample;
		}
	}

	public static class BucketCounts{

		private CuratorFramework client;
		private String counterPathBase = "/partition-count-";

		public BucketCounts(String zookeeperHosts){
			client = CuratorUtils.createAndStartClient(zookeeperHosts);
		}

		public BucketCounts(CuratorFramework client){
			this.client = client;
		}

		public void addToBucketCount(int bucketId, int count){
			CuratorUtils.addCounter(client, counterPathBase + bucketId, count);
		}

		public int getBucketCount(int bucketId){
			return CuratorUtils.getCounter(client, counterPathBase + bucketId);
		}
		
		public void setToBucketCount(int bucketId, int count){
			CuratorUtils.setCounter(client, counterPathBase + bucketId, count);
		}

		public void removeBucketCount(int bucketId){
			CuratorUtils.setCounter(client, counterPathBase + bucketId, 0);
		}

		public void close(){
			client.close();
		}

		public CuratorFramework getClient(){
			return this.client;
		}
	}



	public MDIndex clone() throws CloneNotSupportedException;


	/*
	 *
	 * The Build phase of the index
	 *
	 */


	/**
	 * Initialize the index with the maximum number of buckets.
	 *
	 * @param numBuckets;
	 */
	public void initBuild(int numBuckets);


	/**
	 * Insert an entry into the index structure (internal nodes).
	 * This method does not load the actual data into the index.
	 *
	 * @param key
	 */
	public void insert(MDIndexKey key);


	/**
	 * Bulk load the index structure, without loading the actual data.
	 *
	 * TODO: this method does not really fit in our project because it
	 * assumes data to be in memory.
	 *
	 * @param keys
	 */
	public void bulkLoad(MDIndexKey[] keys);



	/*
	 *
	 * The Probe phase of the index
	 *
	 */


	public void initProbe();

	/**
	 * Get the bucket id, for a given key, from an existing index.
	 *
	 * @param key
	 * @return
	 */
	public Object getBucketId(MDIndexKey key);


//	/**
//	 * Point query.
//	 *
//	 * @param key
//	 * @return the bucket containing the key.
//	 */
//	public Bucket search(MDIndexKey key);
//
//
//	/**
//	 * Range query.
//	 *
//	 * @param low
//	 * @param high
//	 * @return the set of buckets containing the given range.
//	 */
//	public List<Bucket> range(MDIndexKey low, MDIndexKey high);

//	public List<Bucket> search(Predicate[] predicates);

	/*
	 *
	 * Other Utility methods.
	 *
	 */

	/**
	 * Serialize the index into a byte array.
	 *
	 * @return serialized index.
	 */
	public byte[] marshall();


	/**
	 * Deserialize the index from a byte array.
	 *
	 * @param bytes
	 */
	public void unmarshall(byte[] bytes);

	/**
     * Created by qui on 7/9/15.
     */
	class BucketInfo extends Range {

        private int id;

        public BucketInfo(int id, SchemaUtils.TYPE type, Object low, Object high) {
            super(type, low, high);
            this.id = id;
        }

        public BucketInfo(SchemaUtils.TYPE type, Object low, Object high) {
            this(0, type, low, high);
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

		public BucketInfo clone() {
			return new BucketInfo(id, getType(), getLow(), getHigh());
		}
    }
}
