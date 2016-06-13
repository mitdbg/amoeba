package perf.benchmark;

import core.utils.ConfUtils;
import core.utils.TypeUtils.SimpleDate;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

/**
 * Created by ylu on 1/4/16.
 */


public class TPCHSparkJoinWorkload {

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};
    private ConfUtils cfg;
    private String lineitem = "lineitem", orders = "orders", customer = "customer", supplier = "supplier", part = "part";
    private int method;
    private int numQueries;

    private Random rand;

    private JavaSparkContext ctx;
    private SQLContext sqlContext;

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHSparkJoinWorkload t = new TPCHSparkJoinWorkload();
        t.loadSettings(args);
        t.setUp();

        switch (t.method) {
            case 1:
                t.runWorkload();
                break;

            default:
                break;
        }

    }

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        rand = new Random();
        // Making things more deterministic.
        rand.setSeed(0);

        SparkConf sconf = new SparkConf().setMaster(cfg.getSPARK_MASTER())
                .setAppName(this.getClass().getName())
                .setSparkHome(cfg.getSPARK_HOME())
                .setJars(new String[]{cfg.getSPARK_APPLICATION_JAR()})
                .set("spark.hadoop.cloneConf", "false")
                .set("spark.executor.memory", cfg.getSPARK_EXECUTOR_MEMORY())
                .set("spark.driver.memory", cfg.getSPARK_DRIVER_MEMORY())
                .set("spark.task.cpus", cfg.getSPARK_TASK_CPUS());

        try {
            sconf.registerKryoClasses(new Class<?>[]{
                    Class.forName("org.apache.hadoop.io.LongWritable"),
                    Class.forName("org.apache.hadoop.io.Text")
            });
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        ctx = new JavaSparkContext(sconf);
        ctx.hadoopConfiguration().setBoolean(
                FileInputFormat.INPUT_DIR_RECURSIVE, true);
        ctx.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        sqlContext = new SQLContext(ctx);

        // Create customer table
        String customerPath = cfg.getHDFS_WORKING_DIR() + "/" + customer + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE customer (c_custkey int, c_name string, c_address string, "
                + "c_phone string, c_acctbal double, c_mktsegment string , c_nation string, c_region string) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + customerPath + "\", header \"false\", delimiter \"|\")");

        // Create order table.
        String ordersPath = cfg.getHDFS_WORKING_DIR() + "/" + orders + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey int, "
                + "o_orderstatus string, o_totalprice double, o_orderdate string, "
                + "o_orderpriority string, o_clerk string, o_shippriority int) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + ordersPath + "\", header \"false\", delimiter \"|\")");


        // Create lineitem table.
        String lineitemPath = cfg.getHDFS_WORKING_DIR() + "/" + lineitem + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey int, l_suppkey int, "
                + "l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, "
                + "l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, "
                + "l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + lineitemPath + "\", header \"false\", delimiter \"|\")");

        // Create supplier table.
        String supplierPath = cfg.getHDFS_WORKING_DIR() + "/" + supplier + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE supplier (s_suppkey int, s_name string, s_address string, "
                + "s_phone string, s_acctbal double, s_nation string, s_region string) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + supplierPath + "\", header \"false\", delimiter \"|\")");


        // Create path table.
        String partPath = cfg.getHDFS_WORKING_DIR() + "/" + part + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE part (p_partkey int, p_name string, p_mfgr string, p_brand string, "
                + "p_type string, p_size int, p_container string, p_retailprice double) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + partPath + "\", header \"false\", delimiter \"|\")");


    }


    /*
        select
            count(*)
        from
            customer,
            orders,
            lineitem
        where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and c_mktsegment = '[SEGMENT]'
            and o_orderdate < date '[DATE]'
            and l_shipdate > date '[DATE]'

        (lineitem ⋈ orders) ⋈ customer
     */

    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--method":
                    method = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--numQueries":
                    numQueries = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                default:
                    // Something we don't use
                    counter += 2;
                    break;
            }
        }
    }

    /*
        select
	        count(*)
        from
            customer,
            orders,
            lineitem,
            supplier
        where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and l_suppkey = s_suppkey
            and c_region = '[REGION]'
            and s_region = '[REGION]'
            and o_orderdate >= date '[DATE]'
            and o_orderdate < date '[DATE]' + interval '1' year

        ((customer ⋈ orders) ⋈ lineitem) ⋈ supplier
     */

    public void tpch3() {
        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        SimpleDate d3 = new SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));

        String customerPredicate = "c_mktsegment <= \"" + c_mktsegment + "\"";
        String ordersPredicate = "o_orderdate < \"" + d3 + "\"";
        String lineitemPredicate = "l_shipdate > \"" + d3 + "\"";


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            customerPredicate = "c_mktsegment > \"" + c_mktsegment_prev + "\" and " + customerPredicate;
        }

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey "
                + "WHERE " + lineitemPredicate + " and " + ordersPredicate + " and " + customerPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey "
                + "WHERE " + lineitemPredicate + " and " + ordersPredicate + " and " + customerPredicate);

        long result = df.count();  // 29569
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    /*
        select
            count(*)
        from
            lineitem
        where
            l_shipdate >= date '[DATE]'
            and l_shipdate < date '[DATE]' + interval '1' year
            and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
            and l_quantity < [QUANTITY];
     */

    public void tpch5() {
        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        SimpleDate d5_1 = new SimpleDate(year_5, 1, 1);
        SimpleDate d5_2 = new SimpleDate(year_5 + 1, 1, 1);

        String customerPredicate = "c_region <= \"" + r_name_5 + "\"";
        String supplierPredicate = "s_region <= \"" + r_name_5 + "\"";
        String ordersPredicate = "o_orderdate >= \"" + d5_1 + "\" and o_orderdate < \"" + d5_2 + "\"";

        if (rand_5 > 0) {
            String r_name_prev_5 = regionNameVals[rand_5 - 1];
            customerPredicate = "c_region > \"" + r_name_prev_5 + "\" and " + customerPredicate;
            supplierPredicate = "s_region > \"" + r_name_prev_5 + "\" and " + supplierPredicate;
        }

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM customer JOIN orders ON c_custkey = o_custkey "
                + "JOIN lineitem ON l_orderkey = o_orderkey "
                + "JOIN supplier ON l_suppkey = s_suppkey "
                + "WHERE " + customerPredicate + " and " + ordersPredicate + " and " + supplierPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM customer JOIN orders ON c_custkey = o_custkey "
                + "JOIN lineitem ON l_orderkey = o_orderkey "
                + "JOIN supplier ON l_suppkey = s_suppkey "
                + "WHERE " + customerPredicate + " and " + ordersPredicate + " and " + supplierPredicate);

        long result = df.count(); // 35307
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }


    /*
        select
            count(*)
        from
            part,
            lineitem,
            orders,
            customer
        where
            p_partkey = l_partkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_region = '[REGION]'
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = '[TYPE]'

        ((lineitem ⋈ orders) ⋈ customer) ⋈ part
    */

    public void tpch6() {
        int year_6 = 1993 + rand.nextInt(5);
        SimpleDate d6_1 = new SimpleDate(year_6, 1, 1);
        SimpleDate d6_2 = new SimpleDate(year_6 + 1, 1, 1);
        double discount = rand.nextDouble() * 0.07 + 0.02;
        double quantity = rand.nextInt(2) + 24.0;

        String lineitemPredicate = "l_shipdate >= \"" + d6_1 + "\" and l_shipdate < \"" + d6_2 + "\" and "
                + " l_discount > " + (discount - 0.01) + " and l_discount <= " + (discount + 0.01)
                + " and l_quantity <= " + quantity;

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM lineitem "
                + "WHERE " + lineitemPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM lineitem "
                + "WHERE " + lineitemPredicate);

        long result = df.count();  // 83063
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    /*
        select
            count(*)
        from
            orders,
            lineitem,
            customer
        where
            l_orderkey = o_orderkey
            and c_custkey = o_custkey
            and o_orderdate >= date '[DATE]'
            and o_orderdate < date '[DATE]' + interval '3' month
            and l_returnflag = 'R'

        (lineitem ⋈ orders) ⋈ customer
     */

    public void tpch8() {
        int rand_8_1 = rand.nextInt(regionNameVals.length);
        String r_name_8 = regionNameVals[rand_8_1];
        SimpleDate d8_1 = new SimpleDate(1995, 1, 1);
        SimpleDate d8_2 = new SimpleDate(1996, 12, 31);
        String p_type_8 = partTypeVals[rand.nextInt(partTypeVals.length)];


        String customerPredicate = "c_region <= \"" + r_name_8 + "\"";
        String ordersPredicate = "o_orderdate >= \"" + d8_1 + "\" and o_orderdate < \"" + d8_2 + "\"";
        String partPredicate = "p_type = \"" + p_type_8 + "\"";


        if (rand_8_1 > 0) {
            String r_name_prev_8 = regionNameVals[rand_8_1 - 1];
            customerPredicate = "c_region > \"" + r_name_prev_8 + "\" and " + customerPredicate;
        }

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM customer JOIN orders ON c_custkey = o_custkey "
                + "JOIN lineitem ON l_orderkey = o_orderkey "
                + "JOIN part ON l_partkey = p_partkey "
                + "WHERE " + customerPredicate + " and " + ordersPredicate + " and " + partPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM customer JOIN orders ON c_custkey = o_custkey "
                + "JOIN lineitem ON l_orderkey = o_orderkey "
                + "JOIN part ON l_partkey = p_partkey "
                + "WHERE " + customerPredicate + " and " + ordersPredicate + " and " + partPredicate);

        long result = df.count();  // 0
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    /*
        select
            count(*)
        from
            orders,
            lineitem
        where
            o_orderkey = l_orderkey
            and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
            and l_receiptdate >= date '[DATE]'
            and l_receiptdate < date '[DATE]' + interval '1' year

        lineitem ⋈ orders
     */

    public void tpch10() {
        String l_returnflag_10 = "R";
        String l_returnflag_prev_10 = "N";
        int year_10 = 1993;
        int monthOffset = rand.nextInt(24);
        SimpleDate d10_1 = new SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        monthOffset = monthOffset + 3;
        SimpleDate d10_2 = new SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);

        String ordersPredicate = "o_orderdate >= \"" + d10_1 + "\" and o_orderdate < \"" + d10_2 + "\"";
        String lineitemPredicate = "l_returnflag <= \"" + l_returnflag_10 + "\" and l_returnflag > \"" + l_returnflag_prev_10 + "\"";

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM lineitem JOIN orders ON  l_orderkey = o_orderkey "
                + "JOIN customer ON c_custkey = o_custkey "
                + "WHERE " + ordersPredicate + " and " + lineitemPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM lineitem JOIN orders ON  l_orderkey = o_orderkey "
                + "JOIN customer ON c_custkey = o_custkey "
                + "WHERE " + ordersPredicate + " and " + lineitemPredicate);

        long result = df.count();  // 111918
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    /*
        select
            count(*)
        from
            lineitem,
            part
        where
            l_partkey = p_partkey
            and l_shipdate >= date '[DATE]'
            and l_shipdate < date '[DATE]' + interval '1' month;

        lineitem ⋈ part
     */

    public void tpch12() {
        int rand_12 = rand.nextInt(shipModeVals.length);
        String shipmode_12 = shipModeVals[rand_12];
        int year_12 = 1993 + rand.nextInt(5);
        SimpleDate d12_1 = new SimpleDate(year_12, 1, 1);
        SimpleDate d12_2 = new SimpleDate(year_12 + 1, 1, 1);

        String lineitemPredicate = "l_shipmode <= \"" + shipmode_12 + "\" and l_receiptdate >= \"" + d12_1 + "\" and "
                + "l_receiptdate < \"" + d12_2 + "\"";

        if (rand_12 > 0) {
            String shipmode_prev_12 = shipModeVals[rand_12 - 1];
            lineitemPredicate = "l_shipmode > \"" + shipmode_prev_12 + "\" and " + lineitemPredicate;
        }

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM lineitem JOIN orders ON  l_orderkey = o_orderkey "
                + "WHERE " + lineitemPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM lineitem JOIN orders ON  l_orderkey = o_orderkey "
                + "WHERE " + lineitemPredicate);

        long result = df.count(); // 130474
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    /*
        select
            count(*)
        from
            lineitem,
            part
        where
            p_partkey = l_partkey
            and l_shipinstruct = ‘DELIVER IN PERSON’
            and p_brand = ‘[BRAND]’
            and p_container = ‘SM CASE’
            and l_quantity >= [QUANTITY]
            and l_quantity <= [QUANTITY] + 10
            and p_size between 1 and 5
            and l_shipmode <= ‘AIR REG’

        lineitem ⋈ part
     */

    public void tpch14() {
        int year_14 = 1993;
        int monthOffset_14 = rand.nextInt(60);
        SimpleDate d14_1 = new SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        monthOffset_14 += 1;
        SimpleDate d14_2 = new SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);

        String lineitemPredicate = "l_shipdate >= \"" + d14_1 + "\" and l_shipdate < \"" + d14_2 + "\"";

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM lineitem JOIN part ON  l_partkey = p_partkey "
                + "WHERE " + lineitemPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM lineitem JOIN part ON  l_partkey = p_partkey "
                + "WHERE " + lineitemPredicate);

        long result = df.count();  // 76860
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    public void tpch19() {
        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;

        String lineitemPredicate = "l_shipinstruct = \"" + shipInstruct_19 + "\" and l_quantity > " + quantity_19;
        String partPredicate = "p_brand = \"" + brand_19 + "\" and p_container = \"SM CASE\"";
        quantity_19 += 10;

        lineitemPredicate = lineitemPredicate + " and l_quantity <= " + quantity_19 + " and l_shipmode <= \"AIR\"";
        partPredicate = partPredicate + " and p_size >= 1 and p_size <= 5";

        long start = System.currentTimeMillis();

        System.out.println("SELECT * "
                + "FROM lineitem JOIN part ON  l_partkey = p_partkey "
                + "WHERE " + lineitemPredicate + " and " + partPredicate);

        DataFrame df = sqlContext.sql("SELECT * "
                + "FROM lineitem JOIN part ON  l_partkey = p_partkey "
                + "WHERE " + lineitemPredicate + " and " + partPredicate);

        long result = df.count(); // 10
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);
    }

    public void runWorkload() {
        System.out.println("Run TPCH-3");
        rand.setSeed(0);
        tpch3();
        System.out.println("Run TPCH-5");
        rand.setSeed(0);
        tpch5();
        System.out.println("Run TPCH-6");
        rand.setSeed(0);
        tpch6();
        System.out.println("Run TPCH-8");
        rand.setSeed(0);
        tpch8();
        System.out.println("Run TPCH-10");
        rand.setSeed(0);
        tpch10();
        System.out.println("Run TPCH-12");
        rand.setSeed(0);
        tpch12();
        System.out.println("Run TPCH-14");
        rand.setSeed(0);
        tpch14();
        System.out.println("Run TPCH-19");
        rand.setSeed(0);
        tpch19();
    }
}
