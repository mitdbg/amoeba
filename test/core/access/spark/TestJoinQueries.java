package core.access.spark;

import core.access.Predicate;
import core.access.iterator.IteratorRecord;
import core.utils.SchemaUtils;
import junit.framework.TestCase;
import core.index.Settings;
import core.utils.ConfUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;

public class TestJoinQueries extends TestCase {
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);
	public final static int scaleFactor = 1000;
	public static int numQueries = 1;

	double selectivity = 0.05;
	SparkQuery sq;

	public void setUp() {
		sq = new SparkQuery(cfg);
	}

	public void testOrderLineitemJoin() {
		System.out.println("INFO: Running ORDERS, LINEITEM join Query");
		long start = System.currentTimeMillis();
		long result = sq.createJoinRDD("/user/anil/lineitem_orders_join",
				"/user/anil/orders/0",
				0,
				0,
				"/user/anil/repl/0",
				1,
				0
		).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: ORDER-LINEITEM JOIN " + (end - start) + " " + result);
	}

	public void testPartLineitemJoin(){
		System.out.println("INFO: Running PART, LINEITEM join Query");
		long start = System.currentTimeMillis();
		long result = sq.createJoinRDD("/user/anil/lineitem_part_join",
				"/user/anil/part/0",
				0,
				0,
				"/user/anil/repl/0",
				1,
				1
		).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: PART-LINEITEM JOIN " + (end - start) + " " + result);
	}

	public void testScans() {
		System.out.println("INFO: Running ORDERS scan");
		long start = System.currentTimeMillis();
		long result = sq.createScanRDD("/user/anil/orders/0", new Predicate[]{new Predicate(0, SchemaUtils.TYPE.LONG, -1L, Predicate.PREDTYPE.GT)}).count();
		long end = System.currentTimeMillis();
		System.out.println("RES: ORDERS scan " + (end - start) + " " + result);

		System.out.println("INFO: Running PART scan");
		long start2 = System.currentTimeMillis();
		long result2 = sq.createScanRDD("/user/anil/part/0", new Predicate[]{new Predicate(0, SchemaUtils.TYPE.LONG, -1L, Predicate.PREDTYPE.GT)}).count();
		long end2 = System.currentTimeMillis();
		System.out.println("RES: PART scan " + (end2 - start2) + " " + result2);
	}

	public void testBaseline() {
		JavaPairRDD<LongWritable, IteratorRecord> part = sq.createScanRDD("/user/anil/part/0", new Predicate[]{new Predicate(0, SchemaUtils.TYPE.LONG, -1L, Predicate.PREDTYPE.GT)});
		JavaPairRDD<LongWritable, IteratorRecord> lineitem = sq.createScanRDD("/user/anil/repl/0", new Predicate[]{new Predicate(0, SchemaUtils.TYPE.LONG, -1L, Predicate.PREDTYPE.GT)});

		long start = System.currentTimeMillis();
		part.mapToPair(new MapToKeyFunction(0));
		lineitem.mapToPair(new MapToKeyFunction(1));
		System.out.println("JOIN PART-LINEITEM: time to map to keys " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		long result = lineitem.join(part).count();
		System.out.println("JOIN PART-LINEITEM: time "+(System.currentTimeMillis()-start)+" result "+result);
	}

	public void testSmallJoins() {
		String input1 = "/user/anil/part/0";
		String input2 = "/user/anil/repl/0";
		int numChunks = 10;
		int range = 200000 * 1000;
		int chunkSize = range/numChunks;
		double chunkSelectivity = selectivity / numChunks;
		long start = System.currentTimeMillis();
		long total = 0;
		for (int i = 0; i < numChunks; i++) {
			long lowVal = chunkSize * i + (int) (Math.random() * chunkSize * (1 - chunkSelectivity));
			long highVal = lowVal + (int) (chunkSize * chunkSelectivity);
			System.out.println("INFO: getting partkeys in range "+lowVal+" "+highVal);
			long startChunk = System.currentTimeMillis();
			JavaPairRDD<String, String> partRDD = sq.createRDD(
						input1,
						new Predicate(0, SchemaUtils.TYPE.LONG, lowVal, Predicate.PREDTYPE.GT),
						new Predicate(0, SchemaUtils.TYPE.LONG, highVal, Predicate.PREDTYPE.LEQ))
					.mapToPair(new MapToKeyFunction(0));

			JavaPairRDD<String, String> lineRDD = sq.createRDD(
						input2,
						new Predicate(1, SchemaUtils.TYPE.INT, (int) lowVal, Predicate.PREDTYPE.GT),
						new Predicate(1, SchemaUtils.TYPE.INT, (int) highVal, Predicate.PREDTYPE.LEQ))
					.mapToPair(new MapToKeyFunction(1));
			long result = partRDD.join(lineRDD).count();
			total += result;
			long endChunk = System.currentTimeMillis();
			System.out.println("RES: PART range join " + (endChunk - startChunk) + " " + result);
		}
		long end = System.currentTimeMillis();
		System.out.println("RES: OVERALL PART range join " + (end - start) + " " + total);
	}

	public static void main(String[] args) {
		TestJoinQueries tjq = new TestJoinQueries();
		tjq.setUp();
		//tjq.testOrderLineitemJoin();
		tjq.testPartLineitemJoin();
		//tjq.testBaseline();
		//tjq.testSmallJoins();
	}
}
