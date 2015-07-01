package core.access.spark;

import junit.framework.TestCase;
import core.access.spark.SparkQuery;
import core.index.Settings;
import core.utils.ConfUtils;

public class TestJoinQueries extends TestCase {
	public final static String propertyFile = Settings.cartilageConf;
	public final static ConfUtils cfg = new ConfUtils(propertyFile);
	public final static int scaleFactor = 1000;
	public static int numQueries = 1;

	double selectivity;
	SparkQuery sq;
	
	public void setUp() {
		sq = new SparkQuery(cfg);
	}

	public void testOrderLineitemJoin(){
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
	
	public static void main(String[] args) {
		// need to reset the index and removePartitions.sh
		TestJoinQueries tjq = new TestJoinQueries();
		tjq.setUp();
		//tjq.testOrderLineitemJoin();
		tjq.testPartLineitemJoin();
	}
}
