package perf.benchmark;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import perf.tools.BenchmarkSettings;

public class ConvergenceWorkload {
    ConfUtils cfg;

    int method;

    public static void main(String[] args) {
//		BenchmarkSettings.loadSettings(args);
//		BenchmarkSettings.printSettings();
//
//		ConvergenceWorkload tc = new ConvergenceWorkload();
//		tc.loadSettings(args);
//		tc.setUp();
//
//		switch(tc.method) {
//		case 0:
//			tc.testConvergenceShipDateCyclic();
//			break;
//		case 1:
//			tc.testConvergenceShipDateAdHoc();
//			break;
//		case 2:
//			tc.testConvergenceShipDateDrillDown();
//			break;
//		}
    }
//
//	// access shipdate in cyclic pattern, selectivity constant
//	public void testConvergenceShipDateCyclic() {
//		int numQueries = 100;
//		SparkQuery sq = new SparkQuery(cfg);
//		for (int i = 0; i < numQueries; i++) {
//			int year = 1993 + i % 6;
//			long start = System.currentTimeMillis();
//			Predicate p1 = new Predicate(10, TYPE.DATE, new SimpleDate(
//					year - 1, 12, 31), PREDTYPE.GT);
//			Predicate p2 = new Predicate(10, TYPE.DATE, new SimpleDate(year,
//					12, 31), PREDTYPE.LEQ);
//			long c = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
//					.count();
//			long end = System.currentTimeMillis();
//			System.out.println("RES: SHIPDATE " + (end - start) + " " + year
//					+ " " + c);
//		}
//	}
//
//	// access shipdate at random points, selectivity constant
//	public void testConvergenceShipDateAdHoc() {
//		// ORDERDATE uniformly distributed between STARTDATE and (ENDDATE - 151
//		// days)
//		// STARTDATE = 1992-01-01 CURRENTDATE = 1995-06-17 ENDDATE = 1998-12-31
//		// -> between 1992-01-01 and 1998-08-02
//		// SHIPDATE [1...121] days after ORDERDATE -> between 1992-01-02 and
//		// 1998-12-01, 2525 days
//
//		int numQueries = 30;
//		double selectivity = 0.05;
//		SparkQuery sq = new SparkQuery(cfg);
//
//		int range = 2525;
//		Calendar c = new GregorianCalendar();
//		for (int i = 1; i <= numQueries; i++) {
//			int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
//			c.set(1992, Calendar.JANUARY, 02);
//			c.add(Calendar.DAY_OF_MONTH, startOffset);
//			SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
//					c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
//			c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
//			SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
//					c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
//			System.out.println("INFO: Running SHIPDATE Query " + i + " from "
//					+ startDate.toString() + " to " + endDate.toString());
//
//			long start = System.currentTimeMillis();
//			Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
//			Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
//			long result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
//					.count();
//			long end = System.currentTimeMillis();
//			System.out.println("RES: SHIPDATE " + (end - start) + " " + result);
//		}
//	}
//
//	// access shipdate starting with high selectivity and decreasing
//	// exponentially
//	// randomly choose start of interval, which remains the same throughout
//	public void testConvergenceShipDateDrillDown() {
//		int numQueries = 30;
//		double selectivity = 0.1;
//		SparkQuery sq = new SparkQuery(cfg);
//
//		int range = 2525;
//		Calendar c = new GregorianCalendar();
//
//		int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
//		c.set(1992, Calendar.JANUARY, 02);
//		c.add(Calendar.DAY_OF_MONTH, startOffset);
//		SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
//				c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
//		c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
//
//		int reduction = (int) (range * selectivity * -1);
//		for (int i = 1; i <= numQueries; i++) {
//			SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
//					c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
//			System.out.println("INFO: Running SHIPDATE Query " + i + " from "
//					+ startDate.toString() + " to " + endDate.toString());
//
//			long start = System.currentTimeMillis();
//			Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
//			Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
//			long result = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
//					.count();
//			long end = System.currentTimeMillis();
//			System.out.println("RES: SHIPDATE " + (end - start) + " " + result);
//			reduction /= 2;
//			c.add(Calendar.DAY_OF_MONTH, reduction);
//		}
//	}
//
//	public void testConvergenceDiscount() {
//		int numQueries = 15;
//		SparkQuery sq = new SparkQuery(cfg);
//		for (int i = 1; i <= numQueries; i++) {
//			System.out.println("MDINDEX: Running Query " + i);
//			// DISCOUNT - Random (0.02 - 0.09); DISCOUNT+-0.01 range
//			Random r = new Random();
//			double ddisc = r.nextFloat() * 0.07 + 0.02;
//			float disc = (float) ddisc;
//			long start = System.currentTimeMillis();
//			Predicate p1 = new Predicate(6, TYPE.DOUBLE, disc - 0.01,
//					PREDTYPE.GT);
//			Predicate p2 = new Predicate(6, TYPE.DOUBLE, disc + 0.01,
//					PREDTYPE.LEQ);
//			long c = sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
//					.count();
//			long end = System.currentTimeMillis();
//			System.out.println("RES: DISCOUNT " + (end - start) + " " + disc
//					+ " " + c);
//		}
//	}
//
//	public void loadSettings(String[] args) {
//		int counter = 0;
//		while (counter < args.length) {
//			switch (args[counter]) {
//			case "--method":
//				method = Integer.parseInt(args[counter + 1]);
//				counter += 2;
//				break;
//			default:
//				// Something we don't use
//				counter += 2;
//				break;
//			}
//		}
//	}

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);

        // delete query history
        // Cleanup queries file - to remove past query workload
        HDFSUtils.deleteFile(HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
                cfg.getHDFS_WORKING_DIR() + "/queries", false);
    }
}
