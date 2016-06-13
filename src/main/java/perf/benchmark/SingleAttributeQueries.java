package perf.benchmark;

import core.adapt.Predicate;
import core.adapt.Predicate.PREDTYPE;
import core.adapt.Query;
import core.adapt.iterator.IteratorRecord;
import core.adapt.spark.SparkQuery;
import core.utils.ConfUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class SingleAttributeQueries {
    public final static String propertyFile = BenchmarkSettings.conf;
    public final static ConfUtils cfg = new ConfUtils(propertyFile);
    public final static int scaleFactor = 1000;
    public static int numQueries = 1;

    double selectivity;
    SparkQuery sq;

    public static void main(String[] args) {
        SingleAttributeQueries saq = new SingleAttributeQueries();
        saq.selectivity = Double.parseDouble(args[args.length - 1]);
        System.out.println("Started SingleQueryAttributes with selectivity "
                + saq.selectivity);
        saq.setUp();

        saq.testOrderKeyQueries();
        saq.testPartKeyQueries();
        saq.testSupplierKeyQueries();
        saq.testLineNumberQueries();
        saq.testQuantityQueries();
        saq.testExtendedPriceQueries();
        saq.testDiscountQueries();
        saq.testTaxQueries();
        saq.testReturnFlagQuery();
        saq.testLineStatusQueries();
        saq.testShipDateQueries();
        saq.testCommitDateQueries();
        saq.testReceiptDateQueries();
        saq.testShipInstructQuery();
        saq.testShipModeQuery();

        saq.testOrderKeyScan();
        saq.testPartKeyScan();
        saq.testSupplierKeyScan();
        saq.testLineNumberScan();
        saq.testQuantityScan();
        saq.testExtendedPriceScan();
        saq.testDiscountScan();
        saq.testTaxScan();
        saq.testReturnFlagScan();
        saq.testLineStatusScan();
        saq.testShipDateScan();
        saq.testCommitDateScan();
        saq.testReceiptDateScan();
        saq.testShipInstructScan();
        saq.testShipModeScan();
    }

    public void setUp() {
        sq = new SparkQuery(cfg);
    }

    public JavaPairRDD<LongWritable, IteratorRecord> createRDD(String hdfsDir, Predicate... ps) {
        return sq.createNoAdaptRDD(hdfsDir, new Query("lineitem", ps));
    }

    public JavaPairRDD<LongWritable, IteratorRecord> createScanRDD(String hdfsDir, Predicate... ps) {
        return sq.createScanRDD(hdfsDir, new Query("lineitem", ps));
    }

    public void testOrderKeyQueries() {
        int range = scaleFactor * 6000000;
        for (int i = 1; i <= numQueries; i++) {
            long orderKey = (long) (Math.random() * range * (1 - selectivity)) + 1;
            System.out.println("INFO: Running ORDERKEY Query " + i + " from "
                    + orderKey);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(0, TYPE.LONG, orderKey, PREDTYPE.GT);
            Predicate p2 = new Predicate(0, TYPE.LONG, orderKey
                    + (int) (range * selectivity), PREDTYPE.LEQ);
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: ORDERKEY " + (end - start) + " " + result);
        }
    }

    public void testPartKeyQueries() {
        int range = scaleFactor * 200000;
        for (int i = 1; i <= numQueries; i++) {
            int partKey = (int) (Math.random() * range * (1 - selectivity)) + 1;
            System.out.println("INFO: Running PARTKEY Query " + i + " from "
                    + partKey);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(1, TYPE.INT, partKey, PREDTYPE.GT);
            Predicate p2 = new Predicate(1, TYPE.INT, partKey
                    + (int) (range * selectivity), PREDTYPE.LEQ);
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: PARTKEY " + (end - start) + " " + result);
        }
    }

    public void testSupplierKeyQueries() {
        int range = scaleFactor * 10000;
        for (int i = 1; i <= numQueries; i++) {
            int suppKey = (int) (Math.random() * range * (1 - selectivity)) + 1;
            System.out.println("INFO: Running SUPPLIERKEY Query " + i
                    + " from " + suppKey);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(2, TYPE.INT, suppKey, PREDTYPE.GT);
            Predicate p2 = new Predicate(2, TYPE.INT, suppKey
                    + (int) (range * selectivity), PREDTYPE.LEQ);
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: SUPPLIERKEY " + (end - start) + " "
                    + result);
        }
    }

    public void testLineNumberQueries() {
        int range = 7;
        for (int i = 1; i <= numQueries; i++) {
            int lineNum = (int) (Math.random() * range * (1 - selectivity)) + 1;
            int endNum = Math.max(lineNum + 1, lineNum
                    + (int) (range * selectivity));
            System.out.println("INFO: Running LINENUMBER Query " + i + " from "
                    + lineNum + " to " + endNum);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(3, TYPE.INT, lineNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(3, TYPE.INT, endNum, PREDTYPE.LEQ);
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: LINENUMBER " + (end - start) + " "
                    + result);
        }
    }

    public void testQuantityQueries() {
        int range = 50;
        for (int i = 0; i < numQueries; i++) {
            int startNum = (int) (Math.random() * range * (1 - selectivity)) + 1;
            int endNum = Math.max(startNum + 1, startNum
                    + (int) (range * selectivity));
            System.out.println("INFO: Running QUANTITY Query " + i + " from "
                    + startNum + " to " + endNum);
            Predicate p1 = new Predicate(4, TYPE.INT, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(4, TYPE.INT, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: QUANTITY " + (end - start) + " " + result);
        }
    }

    public void testExtendedPriceQueries() {
        // L_EXTENDEDPRICE = L_QUANTITY * P_RETAILPRICE
        // L_QUANTITY: 1 to 50
        // P_RETAILPRICE = (90000 + ((P_PARTKEY/10) modulo 20001 ) + 100 *
        // (P_PARTKEY modulo 1000))/100
        // P_PARTKEY: 1 to scaleFactor * 200000
        // -> range of RETAILPRICE is 900 to 2100
        // -> range of EXTENDEDPRICE is 900 to 105000
        int range = 104100;
        for (int i = 0; i < numQueries; i++) {
            double startNum = (Math.random() * range * (1 - selectivity)) + 900;
            double endNum = Math.max(startNum + 1, startNum + range
                    * selectivity);
            System.out.println("INFO: Running EXTENDEDPRICE Query " + i
                    + " from " + startNum + " to " + endNum);
            Predicate p1 = new Predicate(5, TYPE.DOUBLE, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(5, TYPE.DOUBLE, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: EXTENDEDPRICE " + (end - start) + " "
                    + result);
        }
    }

    public void testDiscountQueries() {
        int range = 10;
        for (int i = 0; i < numQueries; i++) {
            double startNum = (int) (Math.random() * range * (1 - selectivity)) * 0.01;
            double endNum = Math.max(startNum + 0.01, startNum
                    + (int) (range * selectivity) * 0.01);
            System.out.println("INFO: Running DISCOUNT Query " + i + " from "
                    + startNum + " to " + endNum);
            Predicate p1 = new Predicate(6, TYPE.DOUBLE, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(6, TYPE.DOUBLE, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: DISCOUNT " + (end - start) + " " + result);
        }
        // also, > 0 is only 737555 results, not ~811000 (but both are too low)
    }

    public void testTaxQueries() {
        int range = 8;
        for (int i = 0; i < numQueries; i++) {
            double startNum = (int) (Math.random() * range * (1 - selectivity)) * 0.01;
            double endNum = Math.max(startNum + 0.01, startNum
                    + (int) (range * selectivity) * 0.01);
            System.out.println("INFO: Running TAX Query " + i + " from "
                    + startNum + " to " + endNum);
            Predicate p1 = new Predicate(7, TYPE.DOUBLE, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(7, TYPE.DOUBLE, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: TAX " + (end - start) + " " + result);
        }
    }

    public void testReturnFlagQuery() {
        System.out.println("INFO: Running RETURN FLAG Query " + 1);
        Predicate p1 = new Predicate(8, TYPE.STRING, "R", PREDTYPE.EQ);
        long start = System.currentTimeMillis();
        long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: RETURN FLAG " + (end - start) + " " + "R"
                + " " + result);
    }

    public void testLineStatusQueries() {
        System.out.println("INFO: Running LINE STATUS Query " + 1);
        Predicate p1 = new Predicate(9, TYPE.STRING, "O", PREDTYPE.EQ);
        long start = System.currentTimeMillis();
        long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: LINE STATUS " + (end - start) + " " + "O"
                + " " + result);
    }

    public void testShipDateQueries() {
        // ORDERDATE uniformly distributed between STARTDATE and (ENDDATE - 151
        // days)
        // STARTDATE = 1992-01-01 CURRENTDATE = 1995-06-17 ENDDATE = 1998-12-31
        // -> between 1992-01-01 and 1998-08-02
        // SHIPDATE [1...121] days after ORDERDATE -> between 1992-01-02 and
        // 1998-12-01, 2525 days

        int range = 2525;
        Calendar c = new GregorianCalendar();
        for (int i = 1; i <= numQueries; i++) {
            int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
            c.set(1992, Calendar.JANUARY, 02);
            c.add(Calendar.DAY_OF_MONTH, startOffset);
            SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
            SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            System.out.println("INFO: Running SHIPDATE Query " + i + " from "
                    + startDate.toString() + " to " + endDate.toString());

            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
            Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: SHIPDATE " + (end - start) + " " + result);
        }
    }

    public void testCommitDateQueries() {
        // L_COMMITDATE = O_ORDERDATE + random value [30 .. 90]
        // -> between 1992-01-31 and 1998-10-31, 2465 days

        int range = 2465;
        Calendar c = new GregorianCalendar();
        for (int i = 1; i <= numQueries; i++) {
            int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
            c.set(1992, Calendar.JANUARY, 31);
            c.add(Calendar.DAY_OF_MONTH, startOffset);
            SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
            SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            System.out.println("INFO: Running COMMITDATE Query " + i + " from "
                    + startDate.toString() + " to " + endDate.toString());

            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(11, TYPE.DATE, startDate, PREDTYPE.GT);
            Predicate p2 = new Predicate(11, TYPE.DATE, endDate, PREDTYPE.LEQ);
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: COMMITDATE " + (end - start) + " "
                    + result);
        }
    }

    public void testReceiptDateQueries() {
        // L_RECEIPTDATE = L_SHIPDATE + random value [1 .. 30]
        // SHIPDATE between 1992-01-02 and 1998-12-01
        // RECEIPTDATE between 1992-01-03 and 1998-12-31

        int range = 2554;
        Calendar c = new GregorianCalendar();
        for (int i = 0; i < numQueries; i++) {
            int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
            c.set(1992, Calendar.JANUARY, 03);
            c.add(Calendar.DAY_OF_MONTH, startOffset);
            SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
            SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            System.out.println("INFO: Running RECEIPTDATE Query " + i
                    + " from " + startDate.toString() + " to "
                    + endDate.toString());

            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(12, TYPE.DATE, startDate, PREDTYPE.GT);
            Predicate p2 = new Predicate(12, TYPE.DATE, endDate, PREDTYPE.LEQ);
            long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: RECEIPTDATE " + (end - start) + " "
                    + result);
        }
    }

    public void testShipInstructQuery() {
        Predicate p1 = new Predicate(13, TYPE.STRING, "DELIVER IN PERSON",
                PREDTYPE.EQ);
        System.out.println("INFO: Running SHIP INSTRUCT Query " + 1);
        long start = System.currentTimeMillis();
        long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: SHIP INSTRUCT " + (end - start) + " "
                + "DELIVER IN PERSON" + " " + result);
    }

    public void testShipModeQuery() {
        System.out.println("INFO: Running SHIP MODE Query " + 1);
        Predicate p1 = new Predicate(14, TYPE.STRING, "AIR", PREDTYPE.EQ);
        long start = System.currentTimeMillis();
        long result = createRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: SHIP MODE " + (end - start) + " " + "AIR"
                + " " + result);
    }

    public void testOrderKeyScan() {
        int range = scaleFactor * 6000000;
        for (int i = 1; i <= numQueries; i++) {
            long orderKey = (long) (Math.random() * range * (1 - selectivity)) + 1;
            System.out.println("INFO: Running ORDERKEY Query " + i + " from "
                    + orderKey);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(0, TYPE.LONG, orderKey, PREDTYPE.GT);
            Predicate p2 = new Predicate(0, TYPE.LONG, orderKey
                    + (int) (range * selectivity), PREDTYPE.LEQ);
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: ORDERKEY " + (end - start) + " " + result);
        }
    }

    public void testPartKeyScan() {
        int range = scaleFactor * 200000;
        for (int i = 1; i <= numQueries; i++) {
            int partKey = (int) (Math.random() * range * (1 - selectivity)) + 1;
            System.out.println("INFO: Running PARTKEY Query " + i + " from "
                    + partKey);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(1, TYPE.INT, partKey, PREDTYPE.GT);
            Predicate p2 = new Predicate(1, TYPE.INT, partKey
                    + (int) (range * selectivity), PREDTYPE.LEQ);
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: PARTKEY " + (end - start) + " " + result);
        }
    }

    public void testSupplierKeyScan() {
        int range = scaleFactor * 10000;
        for (int i = 1; i <= numQueries; i++) {
            int suppKey = (int) (Math.random() * range * (1 - selectivity)) + 1;
            System.out.println("INFO: Running SUPPLIERKEY Query " + i
                    + " from " + suppKey);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(2, TYPE.INT, suppKey, PREDTYPE.GT);
            Predicate p2 = new Predicate(2, TYPE.INT, suppKey
                    + (int) (range * selectivity), PREDTYPE.LEQ);
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: SUPPLIERKEY " + (end - start) + " "
                    + result);
        }
    }

    public void testLineNumberScan() {
        int range = 7;
        for (int i = 1; i <= numQueries; i++) {
            int lineNum = (int) (Math.random() * range * (1 - selectivity)) + 1;
            int endNum = Math.max(lineNum + 1, lineNum
                    + (int) (range * selectivity));
            System.out.println("INFO: Running LINENUMBER Query " + i + " from "
                    + lineNum + " to " + endNum);
            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(3, TYPE.INT, lineNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(3, TYPE.INT, endNum, PREDTYPE.LEQ);
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: LINENUMBER " + (end - start) + " "
                    + result);
        }
    }

    public void testQuantityScan() {
        int range = 50;
        for (int i = 0; i < numQueries; i++) {
            int startNum = (int) (Math.random() * range * (1 - selectivity)) + 1;
            int endNum = Math.max(startNum + 1, startNum
                    + (int) (range * selectivity));
            System.out.println("INFO: Running QUANTITY Query " + i + " from "
                    + startNum + " to " + endNum);
            Predicate p1 = new Predicate(4, TYPE.INT, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(4, TYPE.INT, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: QUANTITY " + (end - start) + " " + result);
        }
    }

    public void testExtendedPriceScan() {
        // L_EXTENDEDPRICE = L_QUANTITY * P_RETAILPRICE
        // L_QUANTITY: 1 to 50
        // P_RETAILPRICE = (90000 + ((P_PARTKEY/10) modulo 20001 ) + 100 *
        // (P_PARTKEY modulo 1000))/100
        // P_PARTKEY: 1 to scaleFactor * 200000
        // -> range of RETAILPRICE is 900 to 2100
        // -> range of EXTENDEDPRICE is 900 to 105000
        int range = 104100;
        for (int i = 0; i < numQueries; i++) {
            double startNum = (Math.random() * range * (1 - selectivity)) + 900;
            double endNum = Math.max(startNum + 1, startNum + range
                    * selectivity);
            System.out.println("INFO: Running EXTENDEDPRICE Query " + i
                    + " from " + startNum + " to " + endNum);
            Predicate p1 = new Predicate(5, TYPE.DOUBLE, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(5, TYPE.DOUBLE, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: EXTENDEDPRICE " + (end - start) + " "
                    + result);
        }
    }

    public void testDiscountScan() {
        int range = 10;
        for (int i = 0; i < numQueries; i++) {
            double startNum = (int) (Math.random() * range * (1 - selectivity)) * 0.01;
            double endNum = Math.max(startNum + 0.01, startNum
                    + (int) (range * selectivity) * 0.01);
            System.out.println("INFO: Running DISCOUNT Query " + i + " from "
                    + startNum + " to " + endNum);
            Predicate p1 = new Predicate(6, TYPE.DOUBLE, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(6, TYPE.DOUBLE, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: DISCOUNT " + (end - start) + " " + result);
        }
        // also, > 0 is only 737555 results, not ~811000 (but both are too low)
    }

    public void testTaxScan() {
        int range = 8;
        for (int i = 0; i < numQueries; i++) {
            double startNum = (int) (Math.random() * range * (1 - selectivity)) * 0.01;
            double endNum = Math.max(startNum + 0.01, startNum
                    + (int) (range * selectivity) * 0.01);
            System.out.println("INFO: Running TAX Query " + i + " from "
                    + startNum + " to " + endNum);
            Predicate p1 = new Predicate(7, TYPE.DOUBLE, startNum, PREDTYPE.GT);
            Predicate p2 = new Predicate(7, TYPE.DOUBLE, endNum, PREDTYPE.LEQ);
            long start = System.currentTimeMillis();
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: TAX " + (end - start) + " " + result);
        }
    }

    public void testReturnFlagScan() {
        System.out.println("INFO: Running RETURN FLAG Query " + 1);
        Predicate p1 = new Predicate(8, TYPE.STRING, "R", PREDTYPE.EQ);
        long start = System.currentTimeMillis();
        long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: RETURN FLAG " + (end - start) + " " + "R"
                + " " + result);
    }

    public void testLineStatusScan() {
        System.out.println("INFO: Running LINE STATUS Query " + 1);
        Predicate p1 = new Predicate(9, TYPE.STRING, "O", PREDTYPE.EQ);
        long start = System.currentTimeMillis();
        long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: LINE STATUS " + (end - start) + " " + "O"
                + " " + result);
    }

    public void testShipDateScan() {
        // ORDERDATE uniformly distributed between STARTDATE and (ENDDATE - 151
        // days)
        // STARTDATE = 1992-01-01 CURRENTDATE = 1995-06-17 ENDDATE = 1998-12-31
        // -> between 1992-01-01 and 1998-08-02
        // SHIPDATE [1...121] days after ORDERDATE -> between 1992-01-02 and
        // 1998-12-01, 2525 days

        int range = 2525;
        Calendar c = new GregorianCalendar();
        for (int i = 1; i <= numQueries; i++) {
            int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
            c.set(1992, Calendar.JANUARY, 02);
            c.add(Calendar.DAY_OF_MONTH, startOffset);
            SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
            SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            System.out.println("INFO: Running SHIPDATE Query " + i + " from "
                    + startDate.toString() + " to " + endDate.toString());

            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(10, TYPE.DATE, startDate, PREDTYPE.GT);
            Predicate p2 = new Predicate(10, TYPE.DATE, endDate, PREDTYPE.LEQ);
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: SHIPDATE " + (end - start) + " " + result);
        }
    }

    public void testCommitDateScan() {
        // L_COMMITDATE = O_ORDERDATE + random value [30 .. 90]
        // -> between 1992-01-31 and 1998-10-31, 2465 days

        int range = 2465;
        Calendar c = new GregorianCalendar();
        for (int i = 1; i <= numQueries; i++) {
            int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
            c.set(1992, Calendar.JANUARY, 31);
            c.add(Calendar.DAY_OF_MONTH, startOffset);
            SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
            SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            System.out.println("INFO: Running COMMITDATE Query " + i + " from "
                    + startDate.toString() + " to " + endDate.toString());

            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(11, TYPE.DATE, startDate, PREDTYPE.GT);
            Predicate p2 = new Predicate(11, TYPE.DATE, endDate, PREDTYPE.LEQ);
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: COMMITDATE " + (end - start) + " "
                    + result);
        }
    }

    public void testReceiptDateScan() {
        // L_RECEIPTDATE = L_SHIPDATE + random value [1 .. 30]
        // SHIPDATE between 1992-01-02 and 1998-12-01
        // RECEIPTDATE between 1992-01-03 and 1998-12-31

        int range = 2554;
        Calendar c = new GregorianCalendar();
        for (int i = 0; i < numQueries; i++) {
            int startOffset = (int) (Math.random() * range * (1 - selectivity)) + 1;
            c.set(1992, Calendar.JANUARY, 03);
            c.add(Calendar.DAY_OF_MONTH, startOffset);
            SimpleDate startDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            c.add(Calendar.DAY_OF_MONTH, (int) (range * selectivity));
            SimpleDate endDate = new SimpleDate(c.get(Calendar.YEAR),
                    c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
            System.out.println("INFO: Running RECEIPTDATE Query " + i
                    + " from " + startDate.toString() + " to "
                    + endDate.toString());

            long start = System.currentTimeMillis();
            Predicate p1 = new Predicate(12, TYPE.DATE, startDate, PREDTYPE.GT);
            Predicate p2 = new Predicate(12, TYPE.DATE, endDate, PREDTYPE.LEQ);
            long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1, p2)
                    .count();
            long end = System.currentTimeMillis();
            System.out.println("RES: RECEIPTDATE " + (end - start) + " "
                    + result);
        }
    }

    public void testShipInstructScan() {
        Predicate p1 = new Predicate(13, TYPE.STRING, "DELIVER IN PERSON",
                PREDTYPE.EQ);
        System.out.println("INFO: Running SHIP INSTRUCT Query " + 1);
        long start = System.currentTimeMillis();
        long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: SHIP INSTRUCT " + (end - start) + " "
                + "DELIVER IN PERSON" + " " + result);
    }

    // no queries on comment

    public void testShipModeScan() {
        System.out.println("INFO: Running SHIP MODE Query " + 1);
        Predicate p1 = new Predicate(14, TYPE.STRING, "AIR", PREDTYPE.EQ);
        long start = System.currentTimeMillis();
        long result = createScanRDD(cfg.getHDFS_WORKING_DIR(), p1).count();
        long end = System.currentTimeMillis();
        System.out.println("RES: SHIP MODE " + (end - start) + " " + "AIR"
                + " " + result);
    }
}
