package core.access.spark;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Joiner;

import core.access.Predicate;

public class SparkQueryConf {

	public final static String DATASET = "DATASET";
	public final static String FULL_SCAN = "FULL_SCAN";
	public final static String REPARTITION_SCAN = "REPARTITION_SCAN";
	public final static String JUST_ACCESS = "JUST_ACCESS";
	public final static String PREDICATES = "PREDICATES";
	public final static String WORKERS = "WORKERS";
	public final static String MAX_SPLIT_SIZE = "MAX_SPLIT_SIZE";
	public final static String MIN_SPLIT_SIZE = "MIN_SPLIT_SIZE";
	public final static String ZOOKEEPER_HOSTS = "ZOOKEEPER_HOSTS";
	public final static String HADOOP_HOME = "HADOOP_HOME";	// this can be obtained from cartilage properties file
	public final static String COUNTERS_FILE = "COUNTERS_FILE";
	public final static String LOCK_DIR = "LOCK_DIR";
	public final static String NUM_REPLICAS = "NUM_REPLICAS";
	
	
	public final static String CARTILAGE_PROPERTIES = "CARTILAGE_PROPERTIES";

	private Configuration conf;

	public SparkQueryConf(Configuration conf){
		this.conf = conf;
	}

	public void setDataset(String dataset){
		conf.set(DATASET, dataset);
	}

	public String getDataset(){
		return conf.get(DATASET);
	}

	public void setFullScan(boolean flag){
		conf.setBoolean(FULL_SCAN, flag);
	}
	
	public boolean getFullScan(){
		return conf.getBoolean(FULL_SCAN, false);	// don't full scan by default
	}

	public void setRepartitionScan(boolean flag){
		conf.setBoolean(REPARTITION_SCAN, flag);
	}

	public boolean getRepartitionScan(){
		return conf.getBoolean(REPARTITION_SCAN, false);
	}

	public void setJustAccess(boolean flag){
		conf.setBoolean(JUST_ACCESS, flag);
	}
	
	public boolean getJustAccess(){
		return conf.getBoolean(JUST_ACCESS, true);	// don't adapt by default, i.e. just access
	}
	
	public void setPredicates(Predicate[] predicates){
		conf.set(PREDICATES, Joiner.on(",").join(predicates));
	}

	public Predicate[] getPredicates(){
		if (conf.get(PREDICATES).equals("")) {
			return new Predicate[0];
		}
		String[] tokens = conf.get(PREDICATES).split(",");
		Predicate[] predicates = new Predicate[tokens.length];
		for(int i=0; i<predicates.length; i++)
			predicates[i] = new Predicate(tokens[i]);
		return predicates;
	}

	public void setWorkers(int workers){
		conf.set(WORKERS, ""+workers);
	}

	public int getWorkers(){
		return Integer.parseInt(conf.get(WORKERS));
	}

	public void setMaxSplitSize(long maxSplitSize){
		conf.set(MAX_SPLIT_SIZE, ""+maxSplitSize);
	}

	public long getMaxSplitSize(){
		return Long.parseLong(conf.get(MAX_SPLIT_SIZE));
	}

	public void setMinSplitSize(long minSplitSize){
		conf.set(MIN_SPLIT_SIZE, ""+minSplitSize);
	}

	public long getMinSplitSize(){
		return Long.parseLong(conf.get(MIN_SPLIT_SIZE));
	}

	public void setZookeeperHosts(String hosts){
		conf.set(ZOOKEEPER_HOSTS, ""+hosts);
	}

	public String getZookeeperHosts(){
		return conf.get(ZOOKEEPER_HOSTS);
	}

	public void setHadoopHome(String home) {
		conf.set(HADOOP_HOME, home);
	}

	public String getHadoopHome() {
		return conf.get(HADOOP_HOME);
	}
		
	public void setCountersFile(String countersFile) {
		conf.set(COUNTERS_FILE, countersFile);
	}

	public String getCountersFile() {
		return conf.get(COUNTERS_FILE);
	}
	
	public void setLockDir(String lockDir) {
		conf.set(LOCK_DIR, lockDir);
	}

	public String getLockDir() {
		return conf.get(LOCK_DIR);
	}

	public void setCartilageProperties(String cartilageProperties){
		conf.set(CARTILAGE_PROPERTIES, cartilageProperties);
	}

	public String getCartilageProperties(){
		return conf.get(CARTILAGE_PROPERTIES);
	}

	public void setNumReplicas(int numReplicas) { conf.set(NUM_REPLICAS, String.valueOf(numReplicas));}

	public int getNumReplicas() {
		if (conf.get(NUM_REPLICAS) == null) {
			return -1;
		}
		return Integer.parseInt(conf.get(NUM_REPLICAS));}

	public Configuration getConf(){
		return conf;
	}
}
