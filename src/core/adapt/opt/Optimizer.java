package core.adapt.opt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.access.AccessMethod.PartitionSplit;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query;
import core.access.Query.FilterQuery;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PostFilterIterator;
import core.access.iterator.RepartitionIterator;
import core.access.spark.Config;
import core.index.MDIndex;
import core.index.MDIndex.Bucket;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTreeHs;
import core.utils.HDFSUtils;
import core.utils.Pair;
import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

/**
 *
 * @author anil
 *
 */

public class Optimizer {
	RobustTreeHs rt;
	int rtDepth;
	int searchCount;
	public double lambda = 0.2;

	String dataset;
	String hadoopHome;

	static final int BLOCK_SIZE = 64 * 1024;
	static final float DISK_MULTIPLIER = 1;
	static final float NETWORK_MULTIPLIER = 1;

	static final long NODE_MEM_SIZE = 1024 * 1024 * 1024 * 2;
	static final int TUPLE_SIZE = 128;
	static final int NODE_TUPLE_LIMIT = 33554432; // NODE_MEM_SIZE / TUPLE_SIZE

	List<Query> queryWindow = new ArrayList<Query>();

	public static class Plan {
		public Action actions; // Tree of actions
		public float cost;
		public float benefit;

		public Plan() {
			cost = 0;
			benefit = -1;
		}
	}

	public static class Action {
		public int pid; // Predicate
		public int option; // Says which if I used to create this plan
		public Action left;
		public Action right;
	}

	public static class Plans {
		boolean fullAccess;
		Plan PTop; // TODO: Think if this is needed at all
		Plan Best;

		public Plans() {
			this.fullAccess = false;
			this.PTop = null;
			this.Best = null;
		}
	}

	public Optimizer(String dataset, String hadoopHome) {
		this.dataset = dataset;
		this.hadoopHome = hadoopHome;
		searchCount = 0;
	}

	public void loadIndex(String zookeeperHosts) {
		FileSystem fs = HDFSUtils.getFS(hadoopHome + "/etc/hadoop/core-site.xml");
		
		//Bucket.counters = new MDIndex.BucketCounts(zookeeperHosts);
		
		String pathToIndex = this.dataset + "/index";
		String pathToSample = this.dataset + "/sample";
		byte[] indexBytes = HDFSUtils.readFile(fs, pathToIndex);
		this.rt = new RobustTreeHs(0.01);
		this.rt.unmarshall(indexBytes);

		byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
		this.rt.loadSample(sampleBytes);
	}

	public RobustTreeHs getIndex() {
		return rt;
	}

	public int[] getBidFromRNodes(List<RNode> nodes) {
		int[] bids = new int[nodes.size()];
		Iterator<RNode> it = nodes.iterator();
	    for (int i=0; i<bids.length; i++) {
	    	bids[i] = it.next().bucket.getBucketId();
	    }
	    return bids;
	}

	public PartitionSplit[] buildAccessPlan(final Query q) {
		if (q instanceof FilterQuery) {
			FilterQuery fq = (FilterQuery) q;
			List<RNode> nodes = this.rt.getRoot().search(fq.getPredicates());
			PartitionIterator pi = new PostFilterIterator(fq);
			int[] bids = this.getBidFromRNodes(nodes);

			PartitionSplit psplit = new PartitionSplit(bids, pi);
			PartitionSplit[] ps = new PartitionSplit[1];
			ps[0] = psplit;
			return ps;
		} else {
			System.err.println("Unimplemented query - Unable to build plan");
			return null;
		}
	}

	// Just for debug
	public PartitionSplit[] testRepartitionIteratorPlan(final Query q) {
    	Plan pl = new Plan();
    	pl.cost = 1;
    	pl.benefit = 1000;
    	Action ac = new Action();
    	ac.pid = 0;
    	ac.option = 1;
    	pl.actions = ac;

    	Predicate p1 = new Predicate(0, TYPE.INT, 283359707, PREDTYPE.GT);
    	FilterQuery dummy = new FilterQuery(new Predicate[]{p1});

		PartitionSplit[] psplits = this.getPartitionSplits(pl, dummy);
		return psplits;
	}

	public PartitionSplit[] buildPlan(final Query q) {
		if (q instanceof FilterQuery) {
			FilterQuery fq = (FilterQuery) q;
			this.queryWindow.add(fq);
			Plan best = getBestPlan(fq.getPredicates());

			System.out.println("plan.cost: " + best.cost + " plan.benefit: " + best.benefit);
			if (best.cost > best.benefit) {
				best = null;
			}

			PartitionSplit[] psplits;
			if (best != null) {
				psplits = this.getPartitionSplits(best, fq);
			} else {
				psplits = this.buildAccessPlan(fq);
			}

			// Check if we are updating the index ?
			boolean updated = true;
			if (psplits.length == 1) {
				if (psplits[0].getIterator().getClass() == PostFilterIterator.class) {
					updated = false;
				}
			}

			// Debug
			int numTuplesAccessed = 0;
			for (int i=0; i<psplits.length; i++) {
				int[] bids = psplits[i].getPartitions();
				for (int j=0; j<bids.length; j++) {
					numTuplesAccessed += Bucket.counters.getBucketCount(bids[j]);
				}
			}
			System.out.println("Query Cost: " + numTuplesAccessed);

			this.persistQueryToDisk(fq);
			if (updated) {
				System.out.println("INFO: Index being updated");
				this.updateIndex(best, fq.getPredicates());
				this.persistIndexToDisk();
				for (int i = 0; i < psplits.length; i++) {
					if (psplits[i].getIterator().getClass() == RepartitionIterator.class) {
						psplits[i] = new PartitionSplit(psplits[i].getPartitions(), new RepartitionIterator(fq, this.rt.getRoot()));
					}
				}
			} else {
				System.out.println("INFO: No index update");
			}

			return psplits;
		} else {
			System.err.println("Unimplemented query - Unable to build plan");
			return null;
		}
	}

//	public PartitionSplit[] buildMPPlan(final Query q) {
//		if (q instanceof FilterQuery) {
//			FilterQuery fq = (FilterQuery) q;
//			Predicate[] ps = fq.getPredicates();
//			int pLength = ps.length;
//			// Find the initial set of buckets accessed
//			List<RNode> nodes = this.rt.getRoot().search(ps);
//			int[] initialBids = this.getBidFromRNodes(nodes);
//
//			for (int i=0; i<pLength; i++) {
//				Plan best = getBestPlan(ps);
//				this.updateIndex(best, ps);
//				ps[best.actions.pid] = null;
//			}
//
//			nodes = this.rt.getRoot().search(ps);
//			int[] finalBids = this.getBidFromRNodes(nodes);
//
//			float totalTuples = 0;
//			for (int i=0; i<initialBids.length; i++) {
//				int bid = initialBids[i];
//				boolean found = false;
//				for (int j=0; j<finalBids.length; j++) {
//					if (finalBids[j] == bid) {
//						found = true;
//						break;
//					}
//				}
//
//				if (!found) {
//					totalTuples += Bucket.counters.getBucketCount(bid);
//				}
//			}
//
//			return null;
//		} else {
//			System.err.println("Unimplemented query - Unable to build plan");
//			return null;
//		}
//	}

	public Plan getBestPlan(Predicate[] ps) {
		//TODO: Multiple predicates seem to complicate the simple idea we had; think more :-/
		Plan plan = null;
		for (int i=0; i<ps.length; i++) {
			Plan option = getBestPlanForPredicate(ps, i);
			if (plan == null) {
				plan = option;
			} else {
				this.updatePlan(plan, option);
			}
		}

		return plan;
	}

	public void updateIndex(Plan best, Predicate[] ps) {
		this.applyActions(this.rt.getRoot(), best.actions, ps);
	}

	public void applyActions(RNode n, Action a, Predicate[] ps) {
		boolean isRoot = false;
		if (n.parent == null)
			isRoot = true;

		if (a.left != null) {
			this.applyActions(n.leftChild, a.left, ps);
		}

		if (a.right != null) {
			this.applyActions(n.rightChild, a.right, ps);
		}

		switch (a.option) {
		case 0:
			break;
		case 1:
			Predicate p = ps[a.pid];
			RNode r = n.clone();
			r.attribute = p.attribute;
			r.type = p.type;
			r.value = p.getHelpfulCutpoint();
			replaceInTree(n, r);

			// This is the root
			if (isRoot) rt.setRoot(r);

			break;
		case 2:
			Predicate p2 = ps[a.pid];
			assert p2.attribute == n.leftChild.attribute;
			assert p2.attribute == n.rightChild.attribute;
			RNode n2 = n.clone();
			RNode right = n.rightChild.clone();
			replaceInTree(n.leftChild, n2);
			replaceInTree(n, right);
			replaceInTree(right.rightChild, n);
			if (isRoot) rt.setRoot(right);
			break;
		case 3:
			assert a.right == null || a.left == null;
			if (a.left == null) {
				RNode t = n.rightChild;
				RNode tr = t.rightChild;
				RNode tl = t.leftChild;

				replaceInTree(n, t);
				t.rightChild = tr;
				t.rightChild.parent = t;
				t.leftChild = n;
				t.leftChild.parent = t;
				// As side-effect of replace, n.leftChild.parent points to t
				n.leftChild.parent = n;

				n.rightChild = tl;
				n.rightChild.parent = n;

				if (isRoot) rt.setRoot(t);
			} else {
				RNode t = n.leftChild;
				RNode tr = t.rightChild;
				RNode tl = t.leftChild;

				replaceInTree(n, t);
				t.leftChild = tl;
				t.leftChild.parent = t;
				t.rightChild = n;
				t.rightChild.parent = t;
				n.rightChild.parent = n;
				n.leftChild = tr;
				n.leftChild.parent = n;

				if (isRoot) rt.setRoot(t);
			}
			break;
		case 4:
			break;
		case 5:
			break;
		}
	}

	public PartitionSplit[] getPartitionSplits (Plan best, FilterQuery fq) {
		List<PartitionSplit> lps = new ArrayList<PartitionSplit>();
		final int[] modifyingOptions = new int[]{1};
		Action acTree = best.actions;

		LinkedList<RNode> nodeStack = new LinkedList<RNode>();
		nodeStack.add(this.rt.getRoot());

		LinkedList<Action> actionStack = new LinkedList<Action>();
		actionStack.add(acTree);

		List<Integer> unmodifiedBuckets = new ArrayList<Integer>();

		Predicate[] ps = fq.getPredicates();
		while (nodeStack.size() > 0) {
			RNode n = nodeStack.removeLast();
			Action a = actionStack.removeLast();

			boolean isModifying = false;
			for (int t: modifyingOptions){
		        if (t == a.option) {
		        	List<RNode> bs = n.search(ps);
		        	int[] bucketIds = new int[bs.size()];
		        	for (int i=0; i<bucketIds.length; i++) {
		        		bucketIds[i] = bs.get(i).bucket.getBucketId();
		        	}

		        	Predicate p = ps[a.pid];
		        	RNode r = n.clone();
					r.attribute = p.attribute;
					r.type = p.type;
					r.value = p.value;
					replaceInTree(n, r);

					// Give new bucket ids to all nodes below this
					updateBucketIds(bs);

		        	PartitionIterator pi = new RepartitionIterator(fq, r);
		        	PartitionSplit psplit = new PartitionSplit(bucketIds, pi);
		        	lps.add(psplit);
		        	isModifying = true;
		        	break;
		        }
		    }


			if (!isModifying) {
				if (a.right != null) {
					actionStack.add(a.right);
					nodeStack.add(n.rightChild);
				}

				if (a.left != null) {
					actionStack.add(a.left);
					nodeStack.add(n.leftChild);
				}

				if (n.bucket != null) {
					unmodifiedBuckets.add(n.bucket.getBucketId());
				}
			}
		}

		if (unmodifiedBuckets.size() > 0) {
			PartitionIterator pi = new PostFilterIterator(fq);
			int[] bids = new int[unmodifiedBuckets.size()];
			Iterator<Integer> it = unmodifiedBuckets.iterator();
		    for (int i=0; i<bids.length; i++) {
		    	bids[i] = it.next();
		    }
			PartitionSplit psplit = new PartitionSplit(bids, pi);
			lps.add(psplit);
		}

		PartitionSplit[] splits =  lps.toArray(new PartitionSplit[lps.size()]);
		return splits;
	}

	/**
	 * Checks if it is valid to partition on attrId with value val at node
	 * @param node
	 * @param attrId
	 * @param t
	 * @param val
	 * @return
	 */
	public boolean checkValid(RNode node, int attrId, TYPE t, Object val) {
		while (node.parent != null) {
			if (node.parent.attribute == attrId) {
				int ret = TypeUtils.compareTo(node.parent.value, val, t);
				if (node.parent.leftChild == node && ret <= 0) {
					return false;
				} else if (node.parent.rightChild == node && ret >= 0) {
					return false;
				}
			}

			node = node.parent;
		}

		return true;
	}

	/**
	 * Puts the new estimated number of tuples in each bucket after change
	 * @param changed
	 */
	public void populateBucketEstimates(RNode changed) {
		CartilageIndexKeySet collector = null;
		float numTuples = 0;
		int numSamples = 0;

		LinkedList<RNode> stack = new LinkedList<RNode>();
		stack.add(changed);

		while (stack.size() > 0) {
			RNode n = stack.removeLast();
			if (n.bucket != null) {
				CartilageIndexKeySet bucketSample = n.bucket.getSample();
				if (collector == null) {
					collector = new CartilageIndexKeySet();
					collector.setTypes(bucketSample.getTypes());
				}

				numSamples += bucketSample.getValues().size();
				numTuples += n.bucket.getNumTuples();
				collector.addValues(bucketSample.getValues());
			} else {
				stack.add(n.rightChild);
				stack.add(n.leftChild);
			}
		}

		populateBucketEstimates(changed, collector, numTuples/numSamples);
	}

	public void populateBucketEstimates(RNode n, CartilageIndexKeySet sample, float scaleFactor) {
		if (n.bucket != null) {
			n.bucket.estimatedTuples = sample.size() * scaleFactor;
		} else {
			// By sorting we avoid memory allocation
			// Will most probably be faster
			sample.sort(n.attribute);
			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			populateBucketEstimates(n.leftChild, halves.first, scaleFactor);
			populateBucketEstimates(n.rightChild, halves.second, scaleFactor);
		}
	}

	/**
	 * Gives the number of tuples accessed
	 * @param changed
	 * @return
	 */
	public double getNumTuplesAccessed(RNode changed, boolean real) {
		// First traverse to parent to see if query accesses node
		// If yes, find the number of tuples accessed.
		double numTuples = 0;

		int counter = 0;
		for (int i=queryWindow.size() - 1; i >= 0; i--) {
			Query q = queryWindow.get(i);
			double weight = Math.pow(Math.E, - lambda * counter);

			numTuples += weight * getNumTuplesAccessed(changed, q, real);
			counter++;
		}

		return numTuples;
	}

	public float getNumTuplesAccessed(RNode changed, Query q, boolean real) {
		// First traverse to parent to see if query accesses node
		// If yes, find the number of tuples accessed.
		Predicate[] ps = ((FilterQuery)q).getPredicates();

		RNode node = changed;
		boolean accessed = true;
		while (node.parent != null) {
			for (Predicate p: ps) {
				if (p.attribute == node.parent.attribute) {
					if (node.parent.leftChild == node) {
	        			switch (p.predtype) {
	                	case EQ:
	        			case GEQ:
	                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) > 0) accessed = false;
	                		break;
	                	case GT:
	                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) >= 0) accessed = false;
	                		break;
						default:
							break;
	        			}
					} else {
	        			switch (p.predtype) {
	                	case EQ:
	        			case LEQ:
	                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) <= 0) accessed = false;
	                		break;
	                	case LT:
	                		if (TypeUtils.compareTo(p.value, node.parent.value, node.parent.type) < 0) accessed = false;
	                		break;
						default:
							break;
	        			}
					}
				}

				if (!accessed) break;
			}

			if (!accessed) break;
			node = node.parent;
		}

		List<RNode> nodesAccessed = changed.search(ps);
		float tCount = 0;
		for (RNode n: nodesAccessed) {
			//System.out.println("num nodes accessed: "+nodesAccessed.size());
			if (real) {
				tCount += n.bucket.getNumTuples();
			} else {
				tCount += n.bucket.estimatedTuples;
			}
		}

		return tCount;
	}

	/**
	 * Replaces node old by node r in the tree
	 * @param old
	 * @param r
	 */
	public void replaceInTree(RNode old, RNode r) {
        old.leftChild.parent = r;
        old.rightChild.parent = r;
        if (old.parent != null) {
        	if (old.parent.rightChild == old) {
            	old.parent.rightChild = r;
            } else {
            	old.parent.leftChild = r;
            }
        }

        r.leftChild = old.leftChild;
        r.rightChild = old.rightChild;
        r.parent = old.parent;
	}

	public Plan getBestPlanForPredicate(Predicate[] ps, int i) {
		RNode root = rt.getRoot();
		Plans plans = getBestPlanForSubtree(root, ps, i);
		return plans.Best;
	}

	public void updateBucketIds(List<RNode> r) {
		for (RNode n: r) {
			n.bucket.updateId();
		}
	}

	// Update the dest with source if source is a better plan
	public void updatePlan(Plan dest, Plan source) {
		boolean copy = false;
		if (dest.benefit == -1) {
			copy = true;
		} else if (source.cost == 0) {

		} else if (dest.cost == 0) {
			copy = true;
		} else if (dest.benefit / dest.cost < source.benefit / source.cost) {
			copy = true;
		}

		if (copy) {
			dest.benefit = source.benefit;
			dest.cost = source.cost;
			dest.actions = source.actions;
		}
	}

	public Plans getBestPlanForSubtree(RNode node, Predicate[] ps, int pid) {
		// Option Index
		// 1 => Replace
		// 2 => Swap down X
		// 3 =>
		// 5 => Reuse the best plan of subtrees

		if (node.bucket != null) {
			// Leaf
			Plans pl = new Plans();
			pl.fullAccess = true;
			Plan p1 = new Plan();
			p1.cost = 0;
			p1.benefit = 0;
			Action a1 = new Action();
			a1.option = 5;
			p1.actions = a1;
			pl.Best = p1;
			return pl;
		} else {
			Predicate p = ps[pid];
			Plan pTop = new Plan();

			Plan best = new Plan();

			Plans ret = new Plans();

			// Check if both sides are accessed
        	boolean goLeft = true;
        	boolean goRight = true;
        	for (int i = 0; i < ps.length; i++) {
        		Predicate pd = ps[i];
        		if (pd.attribute == node.attribute) {
        			switch (pd.predtype) {
	                	case GEQ:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) > 0) goLeft = false;
	                		break;
	                	case LEQ:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) <= 0) goRight = false;
	                		break;
	                	case GT:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) >= 0) goLeft = false;
	                		break;
	                	case LT:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) < 0) goRight = false;
	                		break;
	                	case EQ:
	                		if (TypeUtils.compareTo(pd.value, node.value, node.type) <= 0) goRight = false;
	                		else goLeft = false;
	                		break;
                	}
        		}
        	}

			Plans leftPlan;
			if (goLeft) {
				leftPlan = getBestPlanForSubtree(node.leftChild, ps, pid);
			} else {
				leftPlan = new Plans();
				leftPlan.Best = null;
				leftPlan.PTop = null;
				leftPlan.fullAccess = false;
			}

			Plans rightPlan;
			if (goRight) {
				rightPlan = getBestPlanForSubtree(node.rightChild, ps, pid);
			} else {
				rightPlan = new Plans();
				rightPlan.Best = null;
				rightPlan.PTop = null;
				rightPlan.fullAccess = false;
			}

			// When trying to replace by predicate; 
			// Replace by testVal, not the actual predicate value
			Object testVal = p.getHelpfulCutpoint();
			
			// replace attribute by one in the predicate
			if (leftPlan.fullAccess && rightPlan.fullAccess) {
				ret.fullAccess = true;

				// If we traverse to root and see that there is no node with cutoff point less than
				// that of predicate, we can do this
				if (checkValid(node, p.attribute, p.type, testVal)) {
					searchCount++;
			        double numAccessedOld = getNumTuplesAccessed(node, true);

					RNode r = node.clone();
					r.attribute = p.attribute;
					r.type = p.type;
					r.value = testVal;
					replaceInTree(node, r);

			        populateBucketEstimates(r);
			        double numAcccessedNew = getNumTuplesAccessed(r, false);
			        double benefit = numAccessedOld - numAcccessedNew;

			        if (benefit > 0) {
			        	// TODO: Better cost model ?
			        	float cost = this.computeCost(r); // Note that buckets haven't changed
			        	Plan pl = new Plan();
			        	pl.cost = cost;
			        	pl.benefit = (float) benefit;
			        	Action ac = new Action();
			        	ac.pid = pid;
			        	ac.option = 1;
			        	pl.actions = ac;

			        	updatePlan(pTop, pl);
			        	updatePlan(best, pl);
			        }

			        // Restore
			        replaceInTree(r, node);
				}
			}

			// Swap down the attribute and bring p above
			if (leftPlan.PTop != null && rightPlan.PTop != null) {
				searchCount++;
				Plan pl = new Plan();
				pl.cost = leftPlan.PTop.cost + rightPlan.PTop.cost;
				pl.benefit = leftPlan.PTop.benefit + rightPlan.PTop.benefit;
				Action ac = new Action();
				ac.pid = pid;
				ac.option = 2;
				ac.left = leftPlan.PTop.actions;
				ac.right = rightPlan.PTop.actions;
				pl.actions = ac;
	        	updatePlan(pTop, pl);
	        	updatePlan(best, pl);
			}

			if (node.attribute == p.attribute) {
				int c = TypeUtils.compareTo(node.value, testVal, node.type);
				if (c != 0) {
					assert (c < 0 && leftPlan.PTop == null) || (c > 0 && rightPlan.PTop == null);
					// Rotate left
					if (c < 0 && rightPlan.PTop != null) {
						searchCount++;
						Plan pl = new Plan();
						pl.cost = rightPlan.PTop.cost;
						pl.benefit = rightPlan.PTop.benefit;
						Action ac = new Action();
						ac.pid = pid;
						ac.option = 3;
						ac.right = rightPlan.PTop.actions;
						pl.actions = ac;
			        	updatePlan(pTop, pl);
			        	updatePlan(best, pl);
					}
					
					//Rotate right
					if (c > 0 && leftPlan.PTop != null) {
						searchCount++;
						Plan pl = new Plan();
						pl.cost = leftPlan.PTop.cost;
						pl.benefit = leftPlan.PTop.benefit;
						Action ac = new Action();
						ac.pid = pid;
						ac.option = 3;
						ac.left = leftPlan.PTop.actions;
						pl.actions = ac;
						updatePlan(pTop, pl);
			        	updatePlan(best, pl);
					}
				}
			}

			// Just Re-Use the Best Plans found for the left/right subtree
			if (leftPlan.Best != null && rightPlan.Best != null) {
				searchCount++;
				Plan pl = new Plan();
				pl.cost = leftPlan.Best.cost + rightPlan.Best.cost;
				pl.benefit = leftPlan.Best.benefit + rightPlan.Best.benefit;
				Action ac = new Action();
				ac.pid = pid;
				ac.option = 5;
				ac.left = leftPlan.Best.actions;
				ac.right = rightPlan.Best.actions;
				pl.actions = ac;
	        	updatePlan(best, pl);
			} else if (rightPlan.Best != null) {
				searchCount++;
				Plan pl = new Plan();
				pl.cost = rightPlan.Best.cost;
				pl.benefit = rightPlan.Best.benefit;
				Action ac = new Action();
				ac.pid = pid;
				ac.option = 5;
				ac.right = rightPlan.Best.actions;
				pl.actions = ac;
	        	updatePlan(best, pl);
			} else if (leftPlan.Best != null) {
				searchCount++;
				Plan pl = new Plan();
				pl.cost = leftPlan.Best.cost;
				pl.benefit = leftPlan.Best.benefit;
				Action ac = new Action();
				ac.pid = pid;
				ac.option = 5;
				ac.left = leftPlan.Best.actions;
				pl.actions = ac;
	        	updatePlan(best, pl);
			}

			if (pTop.benefit != -1) ret.PTop = pTop;
			if (best.benefit != -1) ret.Best = best;

			return ret;
		}
	}

	public float computeCost(RNode r) {
		int numTuples = r.numTuplesInSubtree();
		if (numTuples > NODE_TUPLE_LIMIT) {
			return (DISK_MULTIPLIER + NETWORK_MULTIPLIER) * numTuples;
		} else {
			return DISK_MULTIPLIER * numTuples;
		}
	}

	public static int getDepthOfIndex(int numBlocks) {
		int k = 31 - Integer.numberOfLeadingZeros(numBlocks);
		if (numBlocks == (int)Math.pow(2, k)) {
			return k;
		} else {
			return k+1;
		}
	}

	public int getSearchCount() {
		return searchCount;
	}

	public void loadQueries() {
		FileSystem fs = HDFSUtils.getFS(hadoopHome + "/etc/hadoop/core-site.xml");
		String pathToQueries = this.dataset + "/queries";
		try {
			if (fs.exists(new Path(pathToQueries))) {
				byte[] queryBytes = HDFSUtils.readFile(fs, pathToQueries);
				String queries = new String(queryBytes);
				Scanner sc = new Scanner(queries);
				while (sc.hasNextLine()) {
					String query = sc.nextLine();
					FilterQuery f = new FilterQuery(query);
					queryWindow.add(f);
				}
				sc.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void persistQueryToDisk(FilterQuery fq) {
		String pathToQueries = this.dataset + "/queries";
		HDFSUtils.safeCreateFile(hadoopHome, pathToQueries, Config.replication);
		HDFSUtils.appendLine(hadoopHome, pathToQueries, fq.toString());
	}

	public void persistIndexToDisk() {
		String pathToIndex = this.dataset + "/index";
		FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
		try {			
			if(fs.exists(new Path(pathToIndex))) {
				// If index file exists, move it to a new filename
				long currentMillis = System.currentTimeMillis();
				String oldIndexPath = pathToIndex + "." + currentMillis;
				boolean successRename = fs.rename(new Path(pathToIndex), new Path(oldIndexPath));
				if (!successRename) {
					System.out.println("Index rename to " + oldIndexPath + " failed");
				}
			}	
			HDFSUtils.safeCreateFile(hadoopHome, pathToIndex, Config.replication);
		} catch (IOException e) {
			System.out.println("ERR: Writing Index failed: " + e.getMessage());
			e.printStackTrace();
		}
		
		byte[] indexBytes = this.rt.marshall();
		HDFSUtils.writeFile(HDFSUtils.getFSByHadoopHome(hadoopHome), pathToIndex, (short) 3, this.rt.marshall(), 0, indexBytes.length, false);
	}

	/** Used only in simulator **/
	public void updateCountsBasedOnSample(int totalTuples) {
		this.rt.initializeBucketSamplesAndCounts(this.rt.getRoot(), this.rt.sample, this.rt.sample.size(), totalTuples);
	}
}
