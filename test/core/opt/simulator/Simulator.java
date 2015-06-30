package core.opt.simulator;

import core.access.spark.Config;
import core.utils.CuratorUtils;
import junit.framework.TestCase;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.mapreduce.Job;

import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.adapt.opt.Optimizer;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangeUtils.SimpleDateRange.SimpleDate;
import core.utils.SchemaUtils.TYPE;
import org.apache.hadoop.fs.FileSystem;

public class Simulator extends TestCase{
	String hdfsPath;
	Job job;
	Optimizer opt;
	int sf;
	final int TUPLES_PER_SF = 6000000;

	@Override
	public void setUp(){
		sf = 100;
		//hdfsPath = "hdfs://localhost:9000/user/anil/dodo";
		//hdfsPath = "hdfs://localhost:9000/user/qui/sim";
		hdfsPath = Settings.hdfsPartitionDir;

		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);

		System.out.println("SIMULATOR: Cleaning up");
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		// Cleanup queries file - to remove past query workload
		HDFSUtils.deleteFile(fs, hdfsPath+"/queries", false);

		// Replace index
		byte[] originalIndex = HDFSUtils.readFile(fs, hdfsPath+"/original.index");
		HDFSUtils.writeFile(fs, hdfsPath+"/index", Config.replication, originalIndex, 0, originalIndex.length, false);

		System.out.println("SIMULATOR: clearing bucket counts");
		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		CuratorUtils.stopClient(client);

		System.out.println("SIMULATOR: loading optimizer");
		opt = new Optimizer(hdfsPath, cfg.getHADOOP_HOME());
		opt.loadIndex(cfg.getZOOKEEPER_HOSTS());
	}

	public void testRunQuery(){
		Predicate[] predicates = new Predicate[]{new Predicate(0, TYPE.LONG, 3002147L, PREDTYPE.LEQ)};
		opt.buildPlan(new FilterQuery(predicates));
	}

	public void testSinglePredicateRun() {
		int numQueries = 50;
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			// Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year+1,1,1), PREDTYPE.LT);
			opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
			System.out.println("Updated Bucket Counts");
			opt.buildPlan(new FilterQuery(new Predicate[]{p1}));
			System.out.println("Completed Query " + i);
		}
	}

	public void testCyclicShipDatePredicate() {
		//opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
		int numQueries = 100;
		for (int i=0; i < numQueries; i++) {
			int year = 1993 + i % 6;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
			System.out.println("Updated Bucket Counts");
			long start = System.nanoTime();
			opt.buildPlan(new FilterQuery(new Predicate[]{p1, p2}));
			System.out.println("INFO: Completed Query " + i + " in " + ((System.nanoTime()-start)/1E9));
			System.out.println("INFO: Search count "+opt.getSearchCount());
		}
	}

	public void testRangePredicateRun() {
		int numQueries = 50;
		for (int i=1; i <= numQueries; i++) {
			int year = 1993 + (i + 1) % 5;
			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(year-1,12,31), PREDTYPE.GT);
			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,12,31), PREDTYPE.LEQ);
			opt.updateCountsBasedOnSample(sf * TUPLES_PER_SF);
			System.out.println("INFO: Updated Bucket Counts");
			opt.buildPlan(new FilterQuery(new Predicate[]{p1, p2}));
			System.out.println("INFO: Completed Query " + i);

			if (i == 48) {
				System.out.println("Hola");
			}
		}
	}

	public static void main(String[] args) {
		Simulator s = new Simulator();
		//double[] lambdas = new double[]{0.05, 0.2, 1, 10};
		//double[] lambdas = new double[]{0.2};
		int[] scaleFactors = new int[]{1, 10, 20, 50, 100};
		for (int sf : scaleFactors) {
			s.setUp();
			s.sf = sf;
			//s.opt.lambda = l;
			System.out.println("SIMULATOR: testing cyclic shipdate with scale factor "+sf);
			s.testCyclicShipDatePredicate();
		}
	}
}
