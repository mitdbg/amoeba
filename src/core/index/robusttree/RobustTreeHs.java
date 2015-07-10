package core.index.robusttree;

import java.util.*;

import core.access.Predicate;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.index.key.MDIndexKey;
import core.utils.Pair;
import core.utils.SchemaUtils.TYPE;

public class RobustTreeHs implements MDIndex {
	public int maxBuckets;
	public int numAttributes;
    public CartilageIndexKeySet sample;
    private double samplingRate;

	public TYPE[] dimensionTypes;
	RNode root;

	public static class Bound {
		public double upper;
		public double lower;

		public Bound(double lower, double upper) {
			this.upper = upper;
			this.lower = lower;
		}
	}

	public RobustTreeHs(double samplingRate){
        this.samplingRate = samplingRate;
	}

	@Override
	public MDIndex clone() throws CloneNotSupportedException {
		return new RobustTreeHs(samplingRate);
	}

	public void initBuild(int buckets) {
        this.maxBuckets = buckets;
		root = new RNode();
        this.sample = new CartilageIndexKeySet();
	}

	public void loadSampleAndBuild(int buckets, byte[] bytes) {
        this.maxBuckets = buckets;
		root = new RNode();
        this.sample = new CartilageIndexKeySet();
		this.sample.unmarshall(bytes);

    	this.dimensionTypes = this.sample.getTypes();
        this.numAttributes = this.dimensionTypes.length;
	}

	public RNode getRoot() {
		return root;
	}

	// Only used for testing
	public void setRoot(RNode root) {
		this.root = root;
	}

	@Override
	public boolean equals(Object obj) {
        if(obj instanceof RobustTreeHs){
        	RobustTreeHs rhs = (RobustTreeHs) obj;
        	boolean allGood = true;
        	allGood &= rhs.numAttributes == this.numAttributes;
        	allGood &= rhs.maxBuckets == this.maxBuckets;
        	allGood &= rhs.dimensionTypes.length == this.dimensionTypes.length;
        	if (!allGood)
        		return false;

        	for (int i=0; i<this.dimensionTypes.length; i++) {
        		allGood &= this.dimensionTypes[i] == rhs.dimensionTypes[i];
        	}
        	if (!allGood)
        		return false;

        	allGood = this.root == rhs.root;
        	return allGood;
        }
        return false;
    }

	/***************************************************
	 ************* UPFRONT PARTITIONING ****************
	 ***************************************************/
	public void insert(MDIndexKey key) {
        CartilageIndexKey k = (CartilageIndexKey)key;

		if (dimensionTypes == null) {
        	this.dimensionTypes = k.detectTypes(true);
			if (this.dimensionTypes[0] == TYPE.INT)
				this.dimensionTypes[0] = TYPE.LONG;
            this.numAttributes = dimensionTypes.length;
			this.sample.setTypes(this.dimensionTypes);
        }

        if (Math.random() < samplingRate) {
            this.sample.insert(k);
        }
	}

	public void bulkLoad(MDIndexKey[] keys) {
		for (int i=0; i<keys.length; i++) {
			this.insert(keys[i]);
		}
	}
	
	public class Task {
		RNode node;
		float allocation;
		int depth;
		CartilageIndexKeySet sample;
	}

	/**
	 * Created the tree based on the histograms
	 */
	public void initProbe() {
		System.out.println(this.sample.size() + " keys inserted");

		// Computes log(this.maxBuckets)
		int maxDepth = 31 - Integer.numberOfLeadingZeros(this.maxBuckets); 
		double allocationPerAttribute = RobustTreeHs.nthroot(this.numAttributes, this.maxBuckets);
		System.out.println("Max allocation: " + allocationPerAttribute);

		double[] allocations = new double[this.numAttributes];
		for (int i=0; i<this.numAttributes; i++) {
			allocations[i] = allocationPerAttribute;
		}

		/**
		 * Do a level-order traversal
		 */
		LinkedList<Task> nodeQueue = new LinkedList<Task>();
		// Intialize root with attribute 0
		Task initialTask = new Task();
		initialTask.node = root;
		initialTask.sample = this.sample;
		initialTask.depth = 0;
		nodeQueue.add(initialTask);
		
		while (nodeQueue.size() > 0) {
			Task t = nodeQueue.pollFirst();
			if (t.depth < maxDepth) {
			    int dim = -1;
			    int round = 0;
			    Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = null;

			    while (dim == -1 && round < allocations.length) {
		            int testDim = getLeastAllocated(allocations);
		            allocations[testDim] -= 2.0 / Math.pow(2, t.depth);
		            
		            // TODO: For low cardinality values, it might be better to
					// choose some set of values on each side.
					// TPCH attribute 9 for example has only two distinct values
		            // TODO: This might repeatedly use the same attribute
					halves = t.sample.sortAndSplit(testDim);
					if (halves.first.size() > 0 && halves.second.size() > 0) {
					    dim = testDim;
					} else {
					    System.err.println("WARN: Skipping attribute " + testDim);
					}

					round++;
			    }

			    if (dim == -1) {
		            System.err.println("ERR: No attribute to partition on");
		            Bucket b = new Bucket();
		            b.setSample(sample);
		            t.node.bucket = b;
			    } else {
		            t.node.attribute = dim;
		            t.node.type = this.dimensionTypes[dim];
		            t.node.value = halves.first.getLast(dim); // Need to traverse up for range

	                t.node.leftChild = new RNode();
	                t.node.leftChild.parent = t.node;
	                Task tl = new Task();
	                tl.node = t.node.leftChild;
	                tl.depth = t.depth + 1;
	                tl.sample = halves.first;
	                nodeQueue.add(tl);
	                
	                t.node.rightChild = new RNode();
	                t.node.rightChild.parent = t.node;
	                Task tr = new Task();
	                tr.node = t.node.rightChild;
	                tr.depth = t.depth + 1;
	                tr.sample = halves.second;
	                nodeQueue.add(tr);
			    }
			} else {
			     Bucket b = new Bucket();
			     b.setSample(sample);
			     t.node.bucket = b;
			}
		}
		
		System.out.println("Final Allocations: " + Arrays.toString(allocations));
	}

	// TODO: Add capacity
	static List<Integer> leastAllocated  = new ArrayList<Integer>(20);
	static Random randGenerator = new Random();
	
	/**
	 * Return the dimension which has the maximum
	 * allocation unfulfilled
	 * @return
	 */
	public static int getLeastAllocated(double[] allocations) {
		int numAttributes = allocations.length;

		leastAllocated.clear();
		leastAllocated.add(0);

		double alloc = allocations[0];
		for (int i=1; i < numAttributes; i++) {
			if (allocations[i] > alloc) {
				alloc = allocations[i];
				leastAllocated.clear();
				leastAllocated.add(i);
			} else if (allocations[i] == alloc) {
				leastAllocated.add(i);
			}
		}

		if (leastAllocated.size() == 1) {
			return leastAllocated.get(0);
		} else {
			int r = randGenerator.nextInt(leastAllocated.size());
			return leastAllocated.get(r);
		}
	}

	/**
	 * Used in the 2nd phase of upfront to assign each tuple to the right
	 */
	public Object getBucketId(MDIndexKey key) {
		return root.getBucketId(key);
	}

	/***************************************************
	 ***************** RUNTIME METHODS *****************
	 ***************************************************/

	public List<RNode> getMatchingBuckets(Predicate[] predicates) {
		List<RNode> results = root.search(predicates);
		return results;
	}

	public int getNumTuples(Predicate[] predicates) {
		int total = 0;
		List<RNode> matchingBuckets = getMatchingBuckets(predicates);
		// Note that the above list is a linked-list; don't use .get over it
		for (RNode node: matchingBuckets) {
			total += node.bucket.getNumTuples();
		}

		return total;
	}

	/**
	 * Serializes the index to string
	 * Very brittle - Consider rewriting
	 */
	public byte[] marshall() {
		// JVM optimizes shit so no need to use string builder / buffer
		// Format:
		// maxBuckets, numAttributes
		// types
		// nodes in pre-order

		String robustTree = "";
		robustTree += String.format("%d %d\n", this.maxBuckets, this.numAttributes);

		String types = "";
		for (int i=0; i<this.numAttributes; i++) {
			types += this.dimensionTypes[i].toString() + " ";
		}
		types += "\n";
		robustTree += types;

		robustTree += this.root.marshall();

		return robustTree.getBytes();
	}

	public void unmarshall(byte[] bytes) {
		String tree = new String(bytes);
		Scanner sc = new Scanner(tree);
		this.maxBuckets = sc.nextInt();
		this.numAttributes = sc.nextInt();

		this.dimensionTypes = new TYPE[this.numAttributes];
		for(int i=0; i<this.numAttributes; i++) {
			this.dimensionTypes[i] = TYPE.valueOf(sc.next());
		}

		this.root = new RNode();
		this.root.parseNode(sc);
	}

	public void loadSample(byte[] bytes) {
        this.sample = new CartilageIndexKeySet();
		this.sample.unmarshall(bytes);
		this.initializeBucketSamples(this.root, this.sample);
	}

	public void loadSample(CartilageIndexKeySet sample) {
		this.sample = sample;
		this.dimensionTypes = this.sample.getTypes();
		this.numAttributes = this.dimensionTypes.length;
	}

	public void initializeBucketSamples(RNode n, CartilageIndexKeySet sample) {
		if (n.bucket != null) {
			n.bucket.setSample(sample);
		} else {
			// By sorting we avoid memory allocation
			// Will most probably be faster
			sample.sort(n.attribute);
			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			initializeBucketSamples(n.leftChild, halves.first);
			initializeBucketSamples(n.rightChild, halves.second);
		}
	}

	/** Use only in the simulator **/
	public void initializeBucketSamplesAndCounts(RNode n, CartilageIndexKeySet sample, final long totalSamples, final long totalTuples) {
		if (n.bucket != null) {
			long sampleSize = sample.size();
			int numTuples = (int)( (sampleSize * totalTuples) / totalSamples);
			n.bucket.setSample(sample);
			Bucket.counters.setToBucketCount(n.bucket.getBucketId(), numTuples);
		} else {
			// By sorting we avoid memory allocation
			// Will most probably be faster
			sample.sort(n.attribute);
			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			initializeBucketSamplesAndCounts(n.leftChild, halves.first, totalSamples, totalTuples);
			initializeBucketSamplesAndCounts(n.rightChild, halves.second, totalSamples, totalTuples);
		}
	}

	public byte[] serializeSample() {
		return this.sample.marshall();
	}

	public static double nthroot(int n, double A) {
		return nthroot(n, A, .001);
	}

	public static double nthroot(int n, double A, double p) {
		if(A < 0) {
			System.err.println("A < 0");// we handle only real positive numbers
			return -1;
		} else if(A == 0) {
			return 0;
		}
		double x_prev = A;
		double x = A / n;  // starting "guessed" value...
		while(Math.abs(x - x_prev) > p) {
			x_prev = x;
			x = ((n - 1.0) * x + A / Math.pow(x, n - 1.0)) / n;
		}
		return x;
	}

	/**
	 * Prints the tree created. Call only after initProbe is done.
	 */
	public void printTree() {
		printNode(root);
	}

	public static void printNode(RNode node) {
		if (node.bucket != null) {
			System.out.format("B");
		} else {
			System.out.format("Node: %d %f { ", node.attribute, node.quantile);
			printNode(node.leftChild);
			System.out.print(" }{ ");
			printNode(node.rightChild);
			System.out.print(" }");
		}
	}

	public Map<Integer, BucketInfo> getBucketRanges(int attribute) {
		return getBucketRangeSubtree(this.getRoot(), attribute);
	}

	private static Map<Integer, BucketInfo> getBucketRangeSubtree(RNode root, int attribute) {
		if (root.bucket != null) {
			Map<Integer, BucketInfo> bucketRanges = new HashMap<Integer, BucketInfo>();
			if (root.rangesByAttribute.get(attribute) != null) {
				bucketRanges.put(root.bucket.getBucketId(), root.rangesByAttribute.get(attribute));
			}
			return bucketRanges;
		} else {
			Map<Integer, BucketInfo> bucketRanges = getBucketRangeSubtree(root.leftChild, attribute);
			bucketRanges.putAll(getBucketRangeSubtree(root.rightChild, attribute));
			return bucketRanges;
		}
	}

	public double[] getAllocations() {
		List<RNode> queue = new ArrayList<RNode>();
		Map<Integer, Double> allocs = new HashMap<Integer, Double>();
		queue.add(this.getRoot());

		int nodeNum = 0;
		RNode filler = new RNode();
		filler.attribute = -1;
		int lastNode = 0;
		while (queue.size() > 0) {
			nodeNum++;
			RNode node = queue.remove(0);
			if (node.bucket != null || node.attribute == -1) {
				if (nodeNum > lastNode * 2) {
					break;
				}
				queue.add(filler);
				queue.add(filler);
				continue;
			}
			lastNode = nodeNum;
			if (!(allocs.containsKey(node.attribute))) {
				allocs.put(node.attribute, 0.0);
			}
			allocs.put(node.attribute, allocs.get(node.attribute) + Math.pow(2, -1 * Math.floor(Math.log(nodeNum) / Math.log(2)) + 1));
			queue.add(node.leftChild);
			queue.add(node.rightChild);
		}
		double[] allocArray = new double[numAttributes];
		for (int i = 0; i < numAttributes; i++) {
			if (!(allocs.containsKey(i))) {
				allocArray[i] = 0;
			} else {
				allocArray[i] = allocs.get(i);
			}
		}
		System.out.println(nodeNum);
		return allocArray;

	}
}
