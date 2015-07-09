package core.access.spark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import core.access.Predicate;
import core.access.iterator.IteratorRecord;
import core.utils.ConfUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

public class SparkQuery {
	private SparkQueryConf queryConf;
	private JavaSparkContext ctx;
	private ConfUtils cfg;

	public SparkQuery(ConfUtils config) {
		this.cfg = config;
		//ctx = new JavaSparkContext(cfg.getSPARK_MASTER(), this.getClass().getName());


		SparkConf sconf = new SparkConf()
								.setMaster(cfg.getSPARK_MASTER())
								.setAppName(this.getClass().getName())
								.setSparkHome(cfg.getSPARK_HOME())
								.setJars(new String[]{cfg.getSPARK_JAR()})
								.set("spark.hadoop.cloneConf", "false")
								.set("spark.executor.memory", "150g")
								.set("spark.driver.memory", "10g")
								.set("spark.task.cpus", "32");
				

		ctx = new JavaSparkContext(sconf);
		//ctx = new JavaSparkContext(cfg.getSPARK_MASTER(), this.getClass().getName(), cfg.getSPARK_HOME(), cfg.getSPARK_JAR());
		ctx.hadoopConfiguration().setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true);
		ctx.hadoopConfiguration().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		queryConf = new SparkQueryConf(ctx.hadoopConfiguration());
	}

//	public void createTextFile(String localPath, Predicate... ps){
//		JavaRDD<String> distFile = ctx.textFile(localPath);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}
//
//	public void createHadoopFile(String hdfsPath, Predicate... ps){
//		JavaPairRDD<LongWritable,Text> distFile = ctx.hadoopFile(hdfsPath, TextInputFormat.class, LongWritable.class, Text.class);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}
//
//	public void createHadoopRDD(String hdfsPath, Predicate... ps){
//		JobConf conf = new JobConf(ctx.hadoopConfiguration());
//		FileInputFormat.setInputPaths(conf, hdfsPath);
//
//		JavaPairRDD<LongWritable,Text> distFile = ctx.hadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}
//	
//	public void createNewAPIHadoopRDD(String hdfsPath, Predicate... ps){
//
//		queryConf.setDataset(hdfsPath);
//		queryConf.setPredicates(predicates);
//		queryConf.setWorkers(cfg.getNUM_RACKS() * cfg.getNODES_PER_RACK() * cfg.getMAP_TASKS());
//		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
//		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
//		queryConf.setMaxSplitSize(4096 / 128);	// number of 64 MB partitions that can fit for each worker (we assume 1GB memory for each worker)
//
//		JavaPairRDD<LongWritable,Text> distFile = ctx.newAPIHadoopFile(
//				cfg.getHADOOP_NAMENODE() +  hdfsPath,
//				org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
//				LongWritable.class,
//				Text.class,
//				ctx.hadoopConfiguration()
//			);
//
//		//JavaPairRDD<LongWritable,Text> distFile = ctx.hadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class);
//		long lines = distFile.count();
//		System.out.println("Number of lines = "+lines);
//	}

	public JavaPairRDD<LongWritable,IteratorRecord> createRDD(String hdfsPath, Predicate... ps){
		return this.createRDD(hdfsPath, -1, ps);
	}

	public JavaPairRDD<LongWritable,IteratorRecord> createRDD(String hdfsPath, int numReplicas, Predicate... ps){
		System.out.println("creating RDD with replica number "+numReplicas);
		queryConf.setDataset(hdfsPath);
		queryConf.setNumReplicas(numReplicas);
		queryConf.setPredicates(ps);
		queryConf.setWorkers(cfg.getNUM_RACKS() * cfg.getNODES_PER_RACK() * cfg.getMAP_TASKS());
		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());
		queryConf.setMaxSplitSize(8589934592l);	// 8gb is the max size for each split (with 8 threads in parallel)
		queryConf.setMinSplitSize(4294967296l);	// 4gb
		//queryConf.setCountersFile(cfg.get("COUNTERS_FILE"));
		//queryConf.setCountersFile(cfg.get("LOCK_DIR"));
		
		// ctx.hadoopConfiguration().setClass(FileInputFormat.PATHFILTER_CLASS, SparkPathFilter.class, PathFilter.class);

//		System.setProperty("spark.executor.memory","4g");
//		System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		return ctx.newAPIHadoopFile(
				cfg.getHADOOP_NAMENODE() +  hdfsPath,
				SparkInputFormat.class,
				LongWritable.class,
				IteratorRecord.class,
				ctx.hadoopConfiguration()
			);
	}
	
	public JavaPairRDD<LongWritable,IteratorRecord> createScanRDD(String hdfsPath, Predicate... ps){
		queryConf.setFullScan(true);
		return createRDD(hdfsPath, ps);
	}
	
	public JavaPairRDD<LongWritable,IteratorRecord> createAdaptRDD(String hdfsPath, Predicate... ps){
		queryConf.setJustAccess(false);
		return createRDD(hdfsPath, ps);
	}

	public JavaPairRDD<LongWritable,IteratorRecord> createRepartitionRDD(String hdfsPath, Predicate... ps){
		queryConf.setRepartitionScan(true);
		return createRDD(hdfsPath, ps);
	}

	public JavaRDD<?> createJoinRDD(String partitionIdFile, String hdfsPath1, int rid1, int joinAttribute1, String hdfsPath2, int rid2, int joinAttribute2){

		ctx.hadoopConfiguration().set("PARTITION_ID_FILE", partitionIdFile);
		ctx.hadoopConfiguration().set("JOIN_INPUT1", hdfsPath1);
		ctx.hadoopConfiguration().set("JOIN_INPUT2", hdfsPath2);
		ctx.hadoopConfiguration().set("JOIN_CONDITION", rid1+"."+joinAttribute1+"="+rid2+"."+joinAttribute2);

		queryConf.setHadoopHome(cfg.getHADOOP_HOME());
		queryConf.setZookeeperHosts(cfg.getZOOKEEPER_HOSTS());

		System.out.println(hdfsPath1 +";"+  hdfsPath2);

		return ctx.newAPIHadoopFile(
				hdfsPath1 +";"+ hdfsPath2,
				SparkJoinInputFormat.class,
				LongWritable.class,
				IteratorRecord.class,
				ctx.hadoopConfiguration())
				.mapPartitions(new StringStringJoin(rid1, joinAttribute1, rid2, joinAttribute2));
	}

	public static abstract class JoinFlatMapFunction<K,V> implements FlatMapFunction<Iterator<Tuple2<LongWritable,IteratorRecord>>, Tuple2<V,V>> {
		private static final long serialVersionUID = 1L;
		private SparkHashJoin<K,V> hj;
		protected int rid1, rid2;
		protected int joinAttribute1, joinAttribute2;
		public JoinFlatMapFunction(int rid1, int joinAttribute1, int rid2, int joinAttribute2){
			this.rid1 = rid1;
			this.rid2 = rid2;
			this.joinAttribute1 = joinAttribute1;
			this.joinAttribute2 = joinAttribute2;
			hj = new SparkHashJoin<K,V>();
		}
		public Iterable<Tuple2<V,V>> call(Iterator<Tuple2<LongWritable, IteratorRecord>> arg0) throws Exception {
			// perform join
			hj.initialize(rid1);	// dataset1 is the left hand relation
			while(arg0.hasNext()){
				Tuple2<LongWritable, IteratorRecord> t = arg0.next();
				int rid = (int)t._1().get();
				hj.add(rid, getKey(rid,t._2()), getValue(rid,t._2()));
			}
			// return result
			return hj.getJoinResults();
		}
		protected abstract K getKey(int rid, IteratorRecord r);
		protected abstract V getValue(int rid, IteratorRecord r);
	}

	public static class StringStringJoin extends JoinFlatMapFunction<String,String>{
		private static final long serialVersionUID = 1L;
		public StringStringJoin(int rid1, int joinAttribute1, int rid2, int joinAttribute2) {
			super(rid1, joinAttribute1, rid2, joinAttribute2);
		}
		protected String getKey(int rid, IteratorRecord r) {
			if(rid==rid1)
				return ""+r.getLongAttribute(joinAttribute1);
			else if(rid==rid2)
				return ""+r.getLongAttribute(joinAttribute2);
			else
				throw new RuntimeException("Unknown relation "+rid);
		}
		protected String getValue(int rid, IteratorRecord r) {
			return r.getKeyString();
		}
	}
}
