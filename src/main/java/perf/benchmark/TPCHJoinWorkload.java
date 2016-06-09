package perf.benchmark;


import core.adapt.spark.RangePartitioner;
import core.adapt.spark.join.SparkJoinQuery;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.Predicate.PREDTYPE;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 * Created by ylu on 12/2/15.
 */


public class TPCHJoinWorkload {

    private ConfUtils cfg;

    private Schema schemaCustomer, schemaLineitem, schemaOrders, schemaPart, schemaSupplier;
    private String stringCustomer, stringLineitem, stringOrders, stringPart, stringSupplier;
    private int sizeCustomer, sizeLineitem, sizeOrders, sizePart, sizeSupplier;
    private TableInfo tableLineitem, tableCustomer, tableOrders, tableSupplier, tablePart;

    private String lineitem = "lineitem", orders = "orders", customer = "customer", supplier = "supplier", part = "part";
    private Predicate[] EmptyPredicates = {};

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};

    private SparkJoinQuery sq;


    private int method;

    private int numQueries;

    private Random rand;


    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        sq = new SparkJoinQuery(cfg);
        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);


        tableLineitem = new TableInfo(lineitem, 0, '|', schemaLineitem);
        tableCustomer = new TableInfo(customer, 0, '|', schemaCustomer);
        tableOrders = new TableInfo(orders, 0, '|', schemaOrders);
        tableSupplier = new TableInfo(supplier, 0, '|', schemaSupplier);
        tablePart = new TableInfo(part, 0, '|', schemaPart);

        String workingDir = cfg.getHDFS_WORKING_DIR();
    }

    public void garbageCollect() {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        tableLineitem.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tableCustomer.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tableOrders.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tableSupplier.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tablePart.gc(cfg.getHDFS_WORKING_DIR(), fs);
    }

    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--schemaCustomer":
                    stringCustomer = args[counter + 1];
                    schemaCustomer = Schema.createSchema(stringCustomer);
                    counter += 2;
                    break;
                case "--schemaLineitem":
                    stringLineitem = args[counter + 1];
                    schemaLineitem = Schema.createSchema(stringLineitem);
                    counter += 2;
                    break;
                case "--schemaOrders":
                    stringOrders = args[counter + 1];
                    schemaOrders = Schema.createSchema(stringOrders);
                    counter += 2;
                    break;
                case "--schemaPart":
                    stringPart = args[counter + 1];
                    schemaPart = Schema.createSchema(stringPart);
                    counter += 2;
                    break;
                case "--schemaSupplier":
                    stringSupplier = args[counter + 1];
                    schemaSupplier = Schema.createSchema(stringSupplier);
                    counter += 2;
                    break;
                case "--sizeCustomer":
                    sizeCustomer = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--sizeLineitem":
                    sizeLineitem = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--sizeOrders":
                    sizeOrders = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--sizePart":
                    sizePart = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--sizeSupplier":
                    sizeSupplier = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
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


    public void cleanup(String path) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        try {
            fs.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void postProcessing(String path, String tableName, Schema schema) {

        /* rename part-0000i to i and create an info file*/

        try {
            FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
            String dest = path + "/data";

            // delete _SUCCESS

            fs.delete(new Path(dest + "/_SUCCESS"), false);
            FileStatus[] fileStatus = fs.listStatus(new Path(dest));

            for (int i = 0; i < fileStatus.length; i++) {
                String oldPath = fileStatus[i].getPath().toString();
                String baseName = FilenameUtils.getBaseName(oldPath);
                String dir = oldPath.substring(0, oldPath.length() - baseName.length());
                String newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));

                fs.rename(new Path(oldPath), new Path(newPath));
            }


            /*  write out a fake (TOTAL_NUM_TUPLES is 0, delimiter is set to '|') info to make HDFSPartition Happy*/

            TableInfo tableInfo = new TableInfo(tableName, 0, '|', schema);
            tableInfo.save(cfg.getHDFS_WORKING_DIR(), cfg.getHDFS_REPLICATION_FACTOR(), fs);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public void tpch3(boolean r_lineitem, boolean r_orders, boolean r_customer) {

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        SimpleDate d3 = new SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p1_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TYPE.STRING, c_mktsegment, PREDTYPE.LEQ);
        Predicate p2_3 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d3, PREDTYPE.LT);
        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d3, PREDTYPE.GT);

        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_3});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            Predicate p4_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TYPE.STRING, c_mktsegment_prev, PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3, p4_3});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3});
        }


        q_c.setForceRepartition(r_customer);
        q_o.setForceRepartition(r_orders);
        q_l.setForceRepartition(r_lineitem);

        System.out.println("INFO: Query_cutomer:" + q_c.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());
        System.out.println("INFO: Query_lineitem:" + q_l.toString());


        long start = System.currentTimeMillis();


        // (lineitem ⋈ orders) ⋈ customer

        String stringLineitem_join_Orders = stringLineitem + ", " + stringOrders;
        Schema schemaLineitem_join_Orders = Schema.createSchema(stringLineitem_join_Orders);

        JavaPairRDD<LongWritable, Text> lineitem_join_orders_rdd = sq.createJoinRDD(lineitem, q_l, "NULL", orders, q_o, "NULL", schemaLineitem_join_Orders.getAttributeId("o_custkey"));
        JavaPairRDD<LongWritable, Text> customer_rdd = sq.createScanRDD(customer, q_c);
        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = lineitem_join_orders_rdd.join(customer_rdd);

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);


        //garbageCollect();
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

        ((customer ⋈ orders) ⋈ (lineitem ⋈ supplier))
     */

    public void tpch5(boolean r_customer, boolean r_orders, boolean r_lineitem, boolean r_supplier) {
        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        SimpleDate d5_1 = new SimpleDate(year_5, 1, 1);
        SimpleDate d5_2 = new SimpleDate(year_5 + 1, 1, 1);
        Predicate p1_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_5, PREDTYPE.LEQ);
        Predicate p2_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TYPE.STRING, r_name_5, PREDTYPE.LEQ);
        Predicate p3_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d5_1, PREDTYPE.GEQ);
        Predicate p4_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d5_2, PREDTYPE.LT);


        JoinQuery q_s = null;
        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p3_5, p4_5});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_suppkey"), EmptyPredicates);


        if (rand_5 > 0) {
            String r_name_prev_5 = regionNameVals[rand_5 - 1];
            Predicate p5_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_prev_5, PREDTYPE.GT);
            Predicate p6_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TYPE.STRING, r_name_prev_5, PREDTYPE.GT);
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5, p6_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5, p5_5});
        } else {
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5});
        }

        System.out.println("INFO: Query_cutomer:" + q_c.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());
        System.out.println("INFO: Query_supplier:" + q_s.toString());


        q_s.setForceRepartition(r_supplier);
        q_c.setForceRepartition(r_customer);
        q_o.setForceRepartition(r_orders);
        q_l.setForceRepartition(r_lineitem);

        long start = System.currentTimeMillis();


        // ((customer ⋈ orders) ⋈ (lineitem ⋈ supplier))

        String stringCustomer_join_Orders = stringCustomer + ", " + stringOrders;
        Schema schemaCustomer_join_Orders = Schema.createSchema(stringCustomer_join_Orders);

        String stringLineitem_join_Supplier = stringLineitem + ", " + stringSupplier;
        Schema schemaLineitem_join_Supplier = Schema.createSchema(stringLineitem_join_Supplier);


        JavaPairRDD<LongWritable, Text> customer_join_orders_rdd = sq.createJoinRDD(customer, q_c, "NULL", orders, q_o, "NULL", schemaCustomer_join_Orders.getAttributeId("o_orderkey"));
        JavaPairRDD<LongWritable, Text> lineitem_join_supplier_rdd = sq.createJoinRDD(lineitem, q_l, "NULL", supplier, q_s, "NULL", schemaLineitem_join_Supplier.getAttributeId("l_orderkey"));

        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = customer_join_orders_rdd.join(lineitem_join_supplier_rdd);

        long result = rdd.count();
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        //garbageCollect();
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

    public void tpch6(boolean r_lineitem) {
        int year_6 = 1993 + rand.nextInt(5);
        SimpleDate d6_1 = new SimpleDate(year_6, 1, 1);
        SimpleDate d6_2 = new SimpleDate(year_6 + 1, 1, 1);
        double discount = rand.nextDouble() * 0.07 + 0.02;
        double quantity = rand.nextInt(2) + 24.0;
        Predicate p1_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d6_1, PREDTYPE.GEQ);
        Predicate p2_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d6_2, PREDTYPE.LT);
        Predicate p3_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TYPE.DOUBLE, discount - 0.01, PREDTYPE.GT);
        Predicate p4_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TYPE.DOUBLE, discount + 0.01, PREDTYPE.LEQ);
        Predicate p5_6 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TYPE.DOUBLE, quantity, PREDTYPE.LEQ);
        JoinQuery q_l = new JoinQuery(lineitem, 0, new Predicate[]{p1_6, p2_6, p3_6, p4_6, p5_6});


        q_l.setForceRepartition(r_lineitem);

        //System.out.println("INFO: Query_lineitem:" + q_l.toString());

        long start = System.currentTimeMillis();


        JavaPairRDD<LongWritable, Text> rdd = sq.createScanRDD(lineitem, q_l);
        long result = rdd.count();
        long end = System.currentTimeMillis();
        System.out.println("RES: Time Taken: " + (end - start) + "; Result: " + result);

        //garbageCollect();
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

        (lineitem ⋈ part) ⋈  (orders ⋈ customer)
    */

    public void tpch8(boolean r_lineitem, boolean r_part, boolean r_orders, boolean r_customer) {

        int rand_8_1 = rand.nextInt(regionNameVals.length);
        String r_name_8 = regionNameVals[rand_8_1];
        SimpleDate d8_1 = new SimpleDate(1995, 1, 1);
        SimpleDate d8_2 = new SimpleDate(1996, 12, 31);
        String p_type_8 = partTypeVals[rand.nextInt(partTypeVals.length)];
        Predicate p1_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_8, PREDTYPE.LEQ);
        Predicate p2_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d8_1, PREDTYPE.GEQ);
        Predicate p3_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d8_2, PREDTYPE.LEQ);
        Predicate p4_8 = new Predicate(schemaPart.getAttributeId("p_type"), TYPE.STRING, p_type_8, PREDTYPE.EQ);

        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p2_8, p3_8});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p4_8});
        JoinQuery q_c = null;
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), EmptyPredicates);

        if (rand_8_1 > 0) {
            String r_name_prev_8 = regionNameVals[rand_8_1 - 1];
            Predicate p5_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TYPE.STRING, r_name_prev_8, PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_8, p5_8});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_8});
        }

        System.out.println("INFO: Query_cutomer:" + q_c.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());
        System.out.println("INFO: Query_part:" + q_p.toString());

        q_o.setForceRepartition(r_orders);
        q_p.setForceRepartition(r_part);
        q_c.setForceRepartition(r_customer);
        q_l.setForceRepartition(r_lineitem);

        long start = System.currentTimeMillis();


        // (lineitem ⋈ (orders ⋈ customer)) ⋈ part


        String stringOrders_join_Customer = stringOrders + ", " + stringCustomer;
        Schema schemaOrders_join_Customer = Schema.createSchema(stringOrders_join_Customer);
        String stringLineitem_join_Part = stringLineitem + ", " + stringPart;
        Schema schemaLineitem_join_Part = Schema.createSchema(stringLineitem_join_Part);


        JavaPairRDD<LongWritable, Text> orders_join_customer_rdd = sq.createJoinRDD(orders, q_o, "NULL", customer, q_c, "NULL", schemaOrders_join_Customer.getAttributeId("o_orderkey"));

        JavaPairRDD<LongWritable, Text> lineitem_join_part_rdd = sq.createJoinRDD(lineitem, q_l, "NULL", part, q_p, "NULL", schemaLineitem_join_Part.getAttributeId("l_orderkey"));
        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = orders_join_customer_rdd.join(lineitem_join_part_rdd);

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        //garbageCollect();
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


    public void tpch10(boolean r_lineitem, boolean r_orders, boolean r_customer) {

        String l_returnflag_10 = "R";
        String l_returnflag_prev_10 = "N";
        int year_10 = 1993;
        int monthOffset = rand.nextInt(24);
        SimpleDate d10_1 = new SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        monthOffset = monthOffset + 3;
        SimpleDate d10_2 = new SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        Predicate p1_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TYPE.STRING, l_returnflag_10, PREDTYPE.LEQ);
        Predicate p4_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TYPE.STRING, l_returnflag_prev_10, PREDTYPE.GT);
        Predicate p2_10 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d10_1, PREDTYPE.GEQ);
        Predicate p3_10 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TYPE.DATE, d10_2, PREDTYPE.LT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_10, p4_10});
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_10, p3_10});
        JoinQuery q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), EmptyPredicates);

        System.out.println("INFO: Query_lineitem:" + q_l.toString());
        System.out.println("INFO: Query_orders:" + q_o.toString());


        q_l.setForceRepartition(r_lineitem);
        q_o.setForceRepartition(r_orders);
        q_c.setForceRepartition(r_customer);


        long start, end;


        String stringLineitem_join_Orders = stringLineitem + ", " + stringOrders;
        Schema schemaLineitem_join_Orders = Schema.createSchema(stringLineitem_join_Orders);

        start = System.currentTimeMillis();

        JavaPairRDD<LongWritable, Text> lineitem_join_orders_rdd = sq.createJoinRDD(lineitem, q_l, "NULL", orders, q_o, "NULL", schemaLineitem_join_Orders.getAttributeId("o_custkey"));
        JavaPairRDD<LongWritable, Text> customer_rdd = sq.createScanRDD(customer, q_c);
        JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = lineitem_join_orders_rdd.join(customer_rdd);

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        //garbageCollect();
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

    public void tpch12(boolean r_lineitem, boolean r_orders) {
        int rand_12 = rand.nextInt(shipModeVals.length);
        String shipmode_12 = shipModeVals[rand_12];
        int year_12 = 1993 + rand.nextInt(5);
        SimpleDate d12_1 = new SimpleDate(year_12, 1, 1);
        SimpleDate d12_2 = new SimpleDate(year_12 + 1, 1, 1);
        Predicate p1_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TYPE.STRING, shipmode_12, PREDTYPE.LEQ);
        Predicate p2_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TYPE.DATE, d12_1, PREDTYPE.GEQ);
        Predicate p3_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TYPE.DATE, d12_2, PREDTYPE.LT);

        JoinQuery q_l = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), EmptyPredicates);

        if (rand_12 > 0) {
            String shipmode_prev_12 = shipModeVals[rand_12 - 1];
            Predicate p4_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TYPE.STRING, shipmode_prev_12, PREDTYPE.GT);
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12, p4_12});
        } else {
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12});
        }

        System.out.println("INFO: Query_lineitem:" + q_l.toString());

        q_l.setForceRepartition(r_lineitem);
        q_o.setForceRepartition(r_orders);


        long start = System.currentTimeMillis();

        // lineitem ⋈ orders


        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinRDD(lineitem, q_l, "NULL", orders, q_o, "NULL", 0);

        long result = rdd.count();
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        //garbageCollect();
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

    public void tpch14(boolean r_lineitem, boolean r_part) {

        int year_14 = 1993;
        int monthOffset_14 = rand.nextInt(60);
        SimpleDate d14_1 = new SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        monthOffset_14 += 1;
        SimpleDate d14_2 = new SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        Predicate p1_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d14_1, PREDTYPE.GEQ);
        Predicate p2_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TYPE.DATE, d14_2, PREDTYPE.LT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_14, p2_14});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), EmptyPredicates);

        System.out.println("INFO: Query_lineitem:" + q_l.toString());


        q_l.setForceRepartition(r_lineitem);
        q_p.setForceRepartition(r_part);

        long start = System.currentTimeMillis();

        // lineitem ⋈ part

        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinRDD(lineitem, q_l, "NULL", part, q_p, "NULL", 0);

        long result = rdd.count();
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        //garbageCollect();
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

    public void tpch19(boolean r_lineitem, boolean r_part) {

        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;
        Predicate p1_19 = new Predicate(schemaLineitem.getAttributeId("l_shipinstruct"), TYPE.STRING, shipInstruct_19, PREDTYPE.EQ);
        Predicate p2_19 = new Predicate(schemaPart.getAttributeId("p_brand"), TYPE.STRING, brand_19, PREDTYPE.EQ);
        Predicate p3_19 = new Predicate(schemaPart.getAttributeId("p_container"), TYPE.STRING, "SM CASE", PREDTYPE.EQ);
        Predicate p4_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TYPE.DOUBLE, quantity_19, PREDTYPE.GT);
        quantity_19 += 10;
        Predicate p5_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TYPE.DOUBLE, quantity_19, PREDTYPE.LEQ);
        Predicate p6_19 = new Predicate(schemaPart.getAttributeId("p_size"), TYPE.INT, 1, PREDTYPE.GEQ);
        Predicate p7_19 = new Predicate(schemaPart.getAttributeId("p_size"), TYPE.INT, 5, PREDTYPE.LEQ);
        Predicate p8_19 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TYPE.STRING, "AIR", PREDTYPE.LEQ);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_19, p4_19, p5_19, p8_19});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p2_19, p3_19, p6_19, p7_19});

        System.out.println("INFO: Query_lineitem:" + q_l.toString());
        System.out.println("INFO: Query_part:" + q_p.toString());

        q_l.setForceRepartition(r_lineitem);
        q_p.setForceRepartition(r_part);


        long start = System.currentTimeMillis();

        // lineitem ⋈ part


        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinRDD(lineitem, q_l, "NULL", part, q_p, "NULL", 0);

        long result = rdd.count();
        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        //garbageCollect();
    }

    public void joinLineitemWithOrders(boolean repartition) {

        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), EmptyPredicates);
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), EmptyPredicates);


        q_o.setForceRepartition(repartition);
        q_l.setForceRepartition(repartition);

        System.out.println("INFO: Query_orders:" + q_o.toString());
        System.out.println("INFO: Query_lineitem:" + q_l.toString());


        long start = System.currentTimeMillis();

        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinRDD(lineitem, q_l, "NULL", orders, q_o, "NULL", 0);

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }

    public void joinLineitemWithPart(boolean repartition) {

        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), EmptyPredicates);
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), EmptyPredicates);


        q_p.setForceRepartition(repartition);
        q_l.setForceRepartition(repartition);

        System.out.println("INFO: Query_part:" + q_p.toString());
        System.out.println("INFO: Query_lineitem:" + q_l.toString());


        long start = System.currentTimeMillis();

        JavaPairRDD<LongWritable, Text> rdd = sq.createJoinRDD(lineitem, q_l, "NULL", part, q_p, "NULL", 0);

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }


    public void runSwitchingWorkload() {
        for (int i = 0; i < 160; i++) {
            if (i < 20) {
                System.out.println("INFO: Running query TPC-H q3");
                tpch3(i == 4, i == 4, i == 4);
            } else if (i < 40) {
                System.out.println("INFO: Running query TPC-H q5");
                tpch5(false, i == 24, i == 24, i == 24);
            } else if (i < 60) {
                System.out.println("INFO: Running query TPC-H q6");
                tpch6(false);
            } else if (i < 80) {
                System.out.println("INFO: Running query TPC-H q8");
                tpch8(i == 64, i == 64, false, false);
            } else if (i < 100) {
                System.out.println("INFO: Running query TPC-H q10");
                tpch10(i == 84, i == 84, false);
            } else if (i < 120) {
                System.out.println("INFO: Running query TPC-H q12");
                tpch12(false, false);
            } else if (i < 140) {
                System.out.println("INFO: Running query TPC-H q14");
                tpch14(i == 124, false);
            } else {
                System.out.println("INFO: Running query TPC-H q19");
                tpch19(false, false);
            }
        }
    }


    public void runShiftingWorkload() {

        //int skipuntil = 13;
        int[] numQueries = new int[20];

        for (int i = 0; i < 140; i++) {
            if (i < 20) {
                double p = (20 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    System.out.println("INFO: Running query TPC-H q3");
                    numQueries[3]++;
                    tpch3(numQueries[3] == 5, numQueries[3] == 5, numQueries[3] == 5);
                } else {
                    System.out.println("INFO: Running query TPC-H q5");
                    numQueries[5]++;
                    tpch5(numQueries[3] + numQueries[5] == 5, numQueries[5] == 5, numQueries[5] == 5, numQueries[5] == 5);
                }
            } else if (i < 40) {
                double p = (40 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    System.out.println("INFO: Running query TPC-H q5");
                    numQueries[5]++;
                    tpch5(numQueries[3] + numQueries[5] == 5, numQueries[5] == 5, numQueries[5] == 5, numQueries[5] == 5);
                } else {
                    System.out.println("INFO: Running query TPC-H q6");
                    numQueries[6]++;
                    tpch6(false);
                }
            } else if (i < 60) {

                double p = (60 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    System.out.println("INFO: Running query TPC-H q6");
                    numQueries[6]++;
                    tpch6(false);
                } else {
                    System.out.println("INFO: Running query TPC-H q8");
                    numQueries[8]++;
                    tpch8(numQueries[8] == 5, numQueries[8] == 5, false, false);
                }
            } else if (i < 80) {

                double p = (80 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    System.out.println("INFO: Running query TPC-H q8");
                    numQueries[8]++;
                    tpch8(numQueries[8] == 5, numQueries[8] == 5, false, false);
                } else {
                    System.out.println("INFO: Running query TPC-H q10");
                    numQueries[10]++;
                    tpch10(numQueries[10] == 5, numQueries[10] == 5, false);
                }
            } else if (i < 100) {
                double p = (100 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    System.out.println("INFO: Running query TPC-H q10");
                    numQueries[10]++;
                    tpch10(numQueries[10] == 5, numQueries[10] == 5, false);
                } else {
                    System.out.println("INFO: Running query TPC-H q12");
                    numQueries[12]++;
                    tpch12(false, false);
                }

            } else if (i < 120) {
                double p = (120 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    System.out.println("INFO: Running query TPC-H q12");
                    numQueries[12]++;
                    tpch12(false, false);
                } else {
                    System.out.println("INFO: Running query TPC-H q14");
                    numQueries[14]++;
                    tpch14(numQueries[14] == 5, false);
                }

            } else if (i < 140) {
                double p = (140 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    System.out.println("INFO: Running query TPC-H q14");
                    numQueries[14]++;
                    tpch14(numQueries[14] == 5, false);
                } else {
                    System.out.println("INFO: Running query TPC-H q19");
                    numQueries[19]++;
                    tpch19(false, false);
                }
            }
        }

    }


    public void runUpfrontWorkload() {

        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q3");
            tpch3(false, false, false);
        }
        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q5");
            tpch5(false, false, false, false);
        }
        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q6");
            tpch6(false);
        }
        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q8");
            tpch8(false, false, false, false);
        }
        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q10");
            tpch10(false, false, false);
        }
        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q12");
            tpch12(false, false);
        }
        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q14");
            tpch14(false, false);
        }
        rand.setSeed(0);
        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q19");
            tpch19(false, false);
        }

    }

    public void runBaseline() {
        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q3");
        tpch3(false, false, false);

        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q5");
        tpch5(false, false, false, false);

        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q6");
        tpch6(false);

        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q8");
        tpch8(false, false, false, false);

        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q10");
        tpch10(false, false, false);

        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q12");
        tpch12(false, false);

        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q14");
        tpch14(false, false);

        rand.setSeed(0);

        System.out.println("INFO: Running query TPC-H q19");
        tpch19(false, false);

    }


    public void runBufferSize() {
        System.out.println("INFO: Running query, repartition");

        joinLineitemWithOrders(true);

        long[] bufferSizes = {32L * 1024 * 1024 * 1024, 16L * 1024 * 1024 * 1024, 8L * 1024 * 1024 * 1024,
                4L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024, 1L * 1024 * 1024 * 1024,
                512 * 1024 * 1024, 256 * 1024 * 1024, 128 * 1024 * 1024, 64 * 1024 * 1024};

        for (int i = 0; i < bufferSizes.length; i++) {
            sq.setBufferSize(bufferSizes[i]);
            System.out.println("INFO: Running query, buffer size: " + bufferSizes[i]);
            joinLineitemWithOrders(false);
        }

    }

    public void runVaryingWindowWorkload() {


        joinLineitemWithPart(true);

        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q14");
            tpch14(false, false);
        }

        for (int i = 0; i < 20; i++) {
            double p = (20 - i) / 20.0;
            if (rand.nextDouble() <= p) {
                System.out.println("INFO: Running query TPC-H q14");
                tpch14(false, false);
            } else {
                System.out.println("INFO: Running query TPC-H q19");
                tpch19(false, false);
            }
        }

        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q19");
            tpch19(false, false);
        }

        for (int i = 0; i < 20; i++) {
            double p = (20 - i) / 20.0;
            if (rand.nextDouble() <= p) {
                System.out.println("INFO: Running query TPC-H q19");
                tpch19(false, false);
            } else {
                System.out.println("INFO: Running query TPC-H q14");
                tpch14(false, false);
            }
        }

        for (int i = 0; i < 10; i++) {
            System.out.println("INFO: Running query TPC-H q14");
            tpch14(false, false);
        }

    }


    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHJoinWorkload t = new TPCHJoinWorkload();
        t.loadSettings(args);
        t.setUp();


        switch (t.method) {
            case 1:
                t.runSwitchingWorkload();
                break;
            case 2:
                t.runShiftingWorkload();
                break;
            case 3:
                t.runUpfrontWorkload();
                break;
            case 4:
                t.runBaseline();
                break;
            case 5:
                t.runBufferSize();
                break;
            case 6:
                t.runVaryingWindowWorkload();
                break;
            case 7:
                t.joinLineitemWithOrders(false);
                break;
            case 8:
                t.garbageCollect();
                break;

            default:
                break;
        }

        //t.garbageCollect();
    }
}
