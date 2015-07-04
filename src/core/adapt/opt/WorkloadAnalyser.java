package core.adapt.opt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import core.access.Predicate;
import core.access.Query;
import core.access.Query.FilterQuery;
import core.index.MDIndex.Bucket;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RNode;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.Pair;
import core.utils.SchemaUtils.TYPE;
import core.utils.TypeUtils;

/**
 * Finds the best index tree for a given workload
 * @author anil
 *
 */
public class WorkloadAnalyser {
	public class Candidate {
		public int attribute;
	    public TYPE type;
	    public Object value;
	    
	    public boolean equals(Object obj) {
	    	if (obj instanceof Candidate) {
	    		Candidate objC = (Candidate) obj;
	    		if (objC.attribute == attribute) {
	    			if (TypeUtils.compareTo(value, objC.value, type) == 0) {
	    				return true;
	    			}
	    		}
	    	}
	    	
	    	return false;
	    }
	}
	
	List<Query> queryWindow = new ArrayList<Query>();
	List<Candidate> candidates = new LinkedList<Candidate>();
	RobustTreeHs rt;
	
	float totalNumTuples;
	float totalNumSamples;
	
	/**
	 * 
	 * @param hdfsHomeDir
	 * @param depth depth of index tree
	 */
	public WorkloadAnalyser(ConfUtils conf, int depth, int SF) {
		String hadoopHome = conf.getHADOOP_HOME();
		String hdfsHomeDir = conf.getHDFS_HOMEDIR();
		String pathToQueries = hdfsHomeDir + "/queries";
		String pathToSample = hdfsHomeDir + "/sample";
		
		totalNumTuples = ((float)6000000) * SF;
		
		FileSystem fs = HDFSUtils.getFS(hadoopHome + "/etc/hadoop/core-site.xml");
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
		
		for (int i=0; i<queryWindow.size(); i++) {
			Query k = queryWindow.get(i);
			if (k instanceof FilterQuery) {
				FilterQuery fq = (FilterQuery) k;
				Predicate[] ps = fq.getPredicates();
				for (int j=0; j<ps.length; j++) {
					Candidate c = new Candidate();
					c.attribute = ps[j].attribute;
					c.type = ps[j].type;
					c.value = ps[j].getHelpfulCutpoint();

					if (!candidates.contains(c)) {
						candidates.add(c);						
					}
				}
			}
		}
		
		rt = new RobustTreeHs(0.01);
		rt.initBuild(16);
		byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
		rt.sample.unmarshall(sampleBytes);
		
		totalNumSamples = rt.sample.size();

		buildOptTree(rt.getRoot(), rt.sample, depth);
	}
	
	public float numEstimatedTuples(float numSamples) {
		return numSamples * totalNumTuples / totalNumSamples;
	}
	
	public void buildOptTree(RNode n, CartilageIndexKeySet sample, int depth) {
		n.bucket = new Bucket();
		n.bucket.setSample(sample);
		n.bucket.estimatedTuples = numEstimatedTuples(sample.size());

		if (depth == 0) {
			return;
		}

		double numAccessedOld = getNumTuplesAccessed(n);
		
        double benefit = 0;
        Candidate cChosen = null;
        
		n.bucket = null; // Needed for making sure this is not treated as bucket
		n.leftChild = new RNode();
		n.leftChild.parent = n;
		n.rightChild = new RNode();
		n.rightChild.parent = n;
		n.leftChild.bucket = new Bucket();
		n.rightChild.bucket = new Bucket();

		for (Candidate c: candidates) {			
			if (!Optimizer.checkValidToRoot(n, c.attribute, c.type, c.value)) {
				continue;
			}
			
			n.attribute = c.attribute;
			n.type = c.type;
			n.value = c.value;
			
			sample.sort(n.attribute);
			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			n.leftChild.bucket.estimatedTuples = numEstimatedTuples(halves.first.size());
			n.rightChild.bucket.estimatedTuples = numEstimatedTuples(halves.second.size());
			
	        double numAccessedNew = getNumTuplesAccessed(n);
	        double extraBenefit = numAccessedOld - numAccessedNew;
	        
	        if (extraBenefit > benefit) {
	        	benefit = extraBenefit;
	        	cChosen = c;
	        }
		}
		
		if (cChosen == null) {
			n.bucket = new Bucket();
			n.bucket.setSample(sample);
			n.bucket.estimatedTuples = numEstimatedTuples(sample.size());
		} else {
			n.attribute = cChosen.attribute;
			n.type = cChosen.type;
			n.value = cChosen.value;

			Pair<CartilageIndexKeySet, CartilageIndexKeySet> halves = sample.splitAt(n.attribute, n.value);
			n.leftChild.bucket = null;
			n.rightChild.bucket = null;
			buildOptTree(n.leftChild, halves.first, depth - 1);
			buildOptTree(n.rightChild, halves.second, depth -1);
		}
	}
	
	public float getNumTuplesAccessed(RNode changed) {
		float numTuples = 0;
		for (int i=queryWindow.size() - 1; i >= 0; i--) {
			Query q = queryWindow.get(i);
			numTuples += Optimizer.getNumTuplesAccessed(changed, q, false);
		}

		return numTuples;
	}
	
	public List<Float> getPerQueryCost(RNode root) {
		List<Float> costs = new ArrayList<Float>();
		for (int i=0; i < queryWindow.size(); i++) {
			Query q = queryWindow.get(i);
			costs.add(Optimizer.getNumTuplesAccessed(root, q, false));
		}

		return costs;
	}

	public int getNumCandidates() {
		return candidates.size();
	}
	
	public RobustTreeHs getOptTree() {
		return rt;
	}
}
