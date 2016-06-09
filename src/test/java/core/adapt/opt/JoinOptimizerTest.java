package core.adapt.opt;

import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.spark.join.SparkJoinQuery;
import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.common.index.JoinRobustTree;
import core.common.key.RawIndexKey;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

/**
 * Created by ylu on 1/21/16.
 */
public class JoinOptimizerTest {

    private static Random rand = new Random();

    private static String lineitem = "lineitem", orders = "orders", customer = "customer", supplier = "supplier", part = "part";

    private static String MH = "mh", MHL = "mhl", SF = "sf";

    private static String[] mktSegmentVals = new
            String[]{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static String[] regionNameVals = new
            String[]{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
    private static String[] partTypeVals = new
            String[]{"BRASS", "COPPER", "NICKEL", "STEEL", "TIN"};
    private static String[] shipModeVals = new
            String[]{"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};

    private static Schema schemaCustomer, schemaLineitem, schemaOrders, schemaPart, schemaSupplier;
    private static String stringCustomer, stringLineitem, stringOrders, stringPart, stringSupplier;

    private static Schema schemaMH, schemaMHL, schemaSF;
    private static String stringMH, stringMHL, stringSF;

    private static void setup() {
        stringLineitem = "l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string";
        schemaLineitem = Schema.createSchema(stringLineitem);
        stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int";
        schemaOrders = Schema.createSchema(stringOrders);
        stringCustomer = "c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string";
        schemaCustomer = Schema.createSchema(stringCustomer);
        stringSupplier = "s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string";
        schemaSupplier = Schema.createSchema(stringSupplier);
        stringPart = "p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double";
        schemaPart = Schema.createSchema(stringPart);
        stringMH = "mh_id LONG,mh_dataset_id STRING,mh_uploadtime DATE,mh_runtime STRING,mh_trip_start DATE,mh_data_count_minutes STRING,mh_data_count_accel_samples STRING,mh_data_count_netloc_samples STRING,mh_data_count_gps_samples STRING,mh_observed_sample_rate STRING,mh_distance_mapmatched_km STRING,mh_distance_gps_km STRING,mh_ground_truth_present STRING,mh_timing_mapmatch DOUBLE,mh_distance_pct_path_error STRING,mh_build_version STRING,mh_timing_queue_wait DOUBLE,mh_data_trip_length STRING,mh_battery_maximum_level STRING,mh_battery_minimum_level STRING,mh_battery_drain_rate_per_hour STRING,mh_battery_plugged_duration_hours STRING,mh_battery_delay_from_drive_end_seconds STRING,mh_startlat STRING,mh_startlon STRING,mh_endlat STRING,mh_endlon STRING,mh_data_count_output_gps_speeding_points STRING,mh_speeding_slow_gps_points STRING,mh_speeding_10kmh_gps_points STRING,mh_speeding_20kmh_gps_points STRING,mh_speeding_40kmh_gps_points STRING,mh_speeding_80kmh_gps_points STRING,mh_output_accel_valid_minutes STRING,mh_output_gps_moving_minutes STRING,mh_output_gps_moving_and_accel_valid_minutes STRING,mh_data_time_till_first_gps_minutes STRING,mh_score_di_accel STRING,mh_score_di_brake STRING,mh_score_di_turn STRING,mh_score_di_car_motion STRING,mh_score_di_phone_motion STRING,mh_score_di_speeding STRING,mh_score_di_night STRING,mh_star_rating STRING,mh_trip_end DATE,mh_score_di_car_motion_with_accel STRING,mh_score_di_car_motion_with_speeding STRING,mh_score_di_distance_km_with_accel STRING,mh_score_di_distance_km_with_speeding STRING,mh_score_accel_per_sec_ntile STRING,mh_score_brake_per_sec_ntile STRING,mh_score_turn_per_sec_ntile STRING,mh_score_speeding_per_sec_ntile STRING,mh_score_phone_motion_per_sec_ntile STRING,mh_score_accel_per_km_ntile STRING,mh_score_brake_per_km_ntile STRING,mh_score_turn_per_km_ntile STRING,mh_score_speeding_per_km_ntile STRING,mh_score_phone_motion_per_km_ntile STRING,mh_score STRING,mh_distance_prepended_km STRING,mh_recording_start DATE,mh_score_di_distance_km STRING,mh_recording_end DATE,mh_recording_startlat STRING,mh_recording_startlon STRING,mh_display_distance_km STRING,mh_display_trip_start DATE,mh_display_startlat STRING,mh_display_startlon STRING,mh_data_count_gyro_samples STRING,mh_star_rating_accel STRING,mh_star_rating_brake STRING,mh_star_rating_turn STRING,mh_star_rating_speeding STRING,mh_star_rating_phone_motion STRING,mh_is_night STRING,mh_battery_total_drain STRING,mh_battery_total_drain_duration_hours STRING,mh_score_smoothness STRING,mh_score_awareness STRING,mh_star_rating_night STRING,mh_star_rating_smoothness STRING,mh_star_rating_awareness STRING,mh_hide STRING,mh_data_count_tag_accel_samples STRING,mh_quat_i STRING,mh_quat_j STRING,mh_quat_k STRING,mh_quat_r STRING,mh_passenger_star_rating STRING,mh_suspension_damping_ratio STRING,mh_suspension_natural_frequency STRING,mh_suspension_fit_error STRING,mh_driving STRING,mh_trip_mode STRING,mh_classification_confidence STRING,mh_gk_trip_mode STRING,mh_gk_confidence STRING,mh_offroad_trip_mode STRING,mh_offroad_confidence STRING,mh_driver_confidence STRING,mh_timing_processing_preprocessing DOUBLE,mh_timing_processing_gatekeeper DOUBLE,mh_timing_processing_accelpipeline DOUBLE,mh_timing_processing_offroad DOUBLE,mh_timing_processing_suspension DOUBLE,mh_timing_processing_scoring DOUBLE,mh_timing_processing_hitchhiker DOUBLE,mh_data_count_obd_samples STRING,mh_data_count_pressure_samples STRING,mh_raw_sampling_mode STRING,mh_data_count_magnetometer_samples STRING,mh_location_disabled_date DATE";
        schemaMH = Schema.createSchema(stringMH);
        stringMHL = "mhl_dataset_id LONG, mhl_mapmatch_history_id LONG";
        schemaMHL = Schema.createSchema(stringMHL);
        stringSF = "sf_id LONG,sf_uploadtime DATE,sf_deviceid STRING,sf_driveid STRING,sf_state INT,sf_dest_server STRING,sf_companyid INT,sf_hardware_manufacturer STRING,sf_hardware_model STRING,sf_hardware_bootloader STRING,sf_hardware_build STRING,sf_hardware_carrier STRING,sf_android_fw_version STRING,sf_android_api_version STRING,sf_android_codename STRING,sf_android_baseband STRING,sf_raw_hardware_string STRING,sf_raw_os_string STRING,sf_utc_offset_with_dst STRING,sf_app_version STRING,sf_file_format STRING,sf_start_reason STRING,sf_stop_reason STRING,sf_previous_driveid STRING,sf_userid STRING,sf_tag_mac_address STRING,sf_tag_trip_number STRING,sf_primary_driver_app_user_id STRING,sf_tag_last_connection_number STRING,sf_gps_points_lsh_key_1 STRING,sf_gps_points_lsh_key_2 STRING,sf_gps_points_lsh_key_3 STRING,sf_hidden_by_support STRING";
        schemaSF = Schema.createSchema(stringSF);


        rand.setSeed(0);
    }

    private static JoinQuery tpch3() {

        int rand_3 = rand.nextInt(mktSegmentVals.length);
        String c_mktsegment = mktSegmentVals[rand_3];
        Calendar c = new GregorianCalendar();
        int dateOffset = (int) (rand.nextFloat() * (31 + 28 + 31));
        c.set(1995, Calendar.MARCH, 01);
        c.add(Calendar.DAY_OF_MONTH, dateOffset);
        TypeUtils.SimpleDate d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR),
                c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));


        Predicate p1_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TypeUtils.TYPE.STRING, c_mktsegment, Predicate.PREDTYPE.LEQ);
        Predicate p2_3 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.LT);
        Predicate p3_3 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT);

        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{p2_3});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p3_3});


        if (rand_3 > 0) {
            String c_mktsegment_prev = mktSegmentVals[rand_3 - 1];
            Predicate p4_3 = new Predicate(schemaCustomer.getAttributeId("c_mktsegment"), TypeUtils.TYPE.STRING, c_mktsegment_prev, Predicate.PREDTYPE.GT);
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3, p4_3});
        } else {
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_3});
        }
        return q_l;
    }

    public static JoinQuery tpch5() {

        int rand_5 = rand.nextInt(regionNameVals.length);
        String r_name_5 = regionNameVals[rand_5];
        int year_5 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d5_1 = new TypeUtils.SimpleDate(year_5, 1, 1);
        TypeUtils.SimpleDate d5_2 = new TypeUtils.SimpleDate(year_5 + 1, 1, 1);
        Predicate p1_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_5, Predicate.PREDTYPE.LEQ);
        Predicate p2_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TypeUtils.TYPE.STRING, r_name_5, Predicate.PREDTYPE.LEQ);
        Predicate p3_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d5_1, Predicate.PREDTYPE.GEQ);
        Predicate p4_5 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d5_2, Predicate.PREDTYPE.LT);


        JoinQuery q_s = null;
        JoinQuery q_c = null;
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p3_5, p4_5});
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_suppkey"), new Predicate[]{});


        if (rand_5 > 0) {
            String r_name_prev_5 = regionNameVals[rand_5 - 1];
            Predicate p5_5 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT);
            Predicate p6_5 = new Predicate(schemaSupplier.getAttributeId("s_region"), TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT);
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5, p6_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5, p5_5});
        } else {
            q_s = new JoinQuery(supplier, schemaSupplier.getAttributeId("s_suppkey"), new Predicate[]{p2_5});
            q_c = new JoinQuery(customer, schemaCustomer.getAttributeId("c_custkey"), new Predicate[]{p1_5});
        }
        return q_l;

    }


    private static JoinQuery tpch6() {
        int year_6 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d6_1 = new TypeUtils.SimpleDate(year_6, 1, 1);
        TypeUtils.SimpleDate d6_2 = new TypeUtils.SimpleDate(year_6 + 1, 1, 1);
        double discount = rand.nextDouble() * 0.07 + 0.02;
        double quantity = rand.nextInt(2) + 24.0;
        Predicate p1_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d6_1, Predicate.PREDTYPE.GEQ);
        Predicate p2_6 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d6_2, Predicate.PREDTYPE.LT);
        Predicate p3_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TypeUtils.TYPE.DOUBLE, discount - 0.01, Predicate.PREDTYPE.GT);
        Predicate p4_6 = new Predicate(schemaLineitem.getAttributeId("l_discount"), TypeUtils.TYPE.DOUBLE, discount + 0.01, Predicate.PREDTYPE.LEQ);
        Predicate p5_6 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity, Predicate.PREDTYPE.LEQ);
        JoinQuery q_l = new JoinQuery(lineitem, 0, new Predicate[]{p1_6, p2_6, p3_6, p4_6, p5_6});

        return q_l;

    }

    private static JoinQuery tpch8() {
        int rand_8_1 = rand.nextInt(regionNameVals.length);
        String r_name_8 = regionNameVals[rand_8_1];
        TypeUtils.SimpleDate d8_1 = new TypeUtils.SimpleDate(1995, 1, 1);
        TypeUtils.SimpleDate d8_2 = new TypeUtils.SimpleDate(1996, 12, 31);
        String p_type_8 = partTypeVals[rand.nextInt(partTypeVals.length)];
        Predicate p1_8 = new Predicate(schemaCustomer.getAttributeId("c_region"), TypeUtils.TYPE.STRING, r_name_8, Predicate.PREDTYPE.LEQ);
        Predicate p2_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d8_1, Predicate.PREDTYPE.GEQ);
        Predicate p3_8 = new Predicate(schemaOrders.getAttributeId("o_orderdate"), TypeUtils.TYPE.DATE, d8_2, Predicate.PREDTYPE.LEQ);
        Predicate p4_8 = new Predicate(schemaPart.getAttributeId("p_type"), TypeUtils.TYPE.STRING, p_type_8, Predicate.PREDTYPE.EQ);

        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_custkey"), new Predicate[]{p2_8, p3_8});
        JoinQuery q_p = new JoinQuery(part, schemaPart.getAttributeId("p_partkey"), new Predicate[]{p4_8});
        JoinQuery q_c = null;
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{});

        return q_l;
    }

    private static JoinQuery tpch10() {

        String l_returnflag_10 = "R";
        String l_returnflag_prev_10 = "N";
        int year_10 = 1993;
        int monthOffset = rand.nextInt(24);
        TypeUtils.SimpleDate d10_1 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        monthOffset = monthOffset + 3;
        TypeUtils.SimpleDate d10_2 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1);
        Predicate p1_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TypeUtils.TYPE.STRING, l_returnflag_10, Predicate.PREDTYPE.LEQ);
        Predicate p4_10 = new Predicate(schemaLineitem.getAttributeId("l_returnflag"), TypeUtils.TYPE.STRING, l_returnflag_prev_10, Predicate.PREDTYPE.GT);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_10, p4_10});
        return q_l;
    }

    private static JoinQuery tpch12() {
        int rand_12 = rand.nextInt(shipModeVals.length);
        String shipmode_12 = shipModeVals[rand_12];
        int year_12 = 1993 + rand.nextInt(5);
        TypeUtils.SimpleDate d12_1 = new TypeUtils.SimpleDate(year_12, 1, 1);
        TypeUtils.SimpleDate d12_2 = new TypeUtils.SimpleDate(year_12 + 1, 1, 1);
        Predicate p1_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, shipmode_12, Predicate.PREDTYPE.LEQ);
        Predicate p2_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TypeUtils.TYPE.DATE, d12_1, Predicate.PREDTYPE.GEQ);
        Predicate p3_12 = new Predicate(schemaLineitem.getAttributeId("l_receiptdate"), TypeUtils.TYPE.DATE, d12_2, Predicate.PREDTYPE.LT);

        JoinQuery q_l = null;

        if (rand_12 > 0) {
            String shipmode_prev_12 = shipModeVals[rand_12 - 1];
            Predicate p4_12 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, shipmode_prev_12, Predicate.PREDTYPE.GT);
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12, p4_12});
        } else {
            q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p1_12, p2_12, p3_12});
        }

        q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{p2_12, p3_12});


        System.out.println("INFO: Query_lineitem:" + q_l.toString());

        return q_l;

    }

    private static JoinQuery tpch14() {
        int year_14 = 1993;
        int monthOffset_14 = rand.nextInt(60);
        TypeUtils.SimpleDate d14_1 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        monthOffset_14 += 1;
        TypeUtils.SimpleDate d14_2 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1);
        Predicate p1_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d14_1, Predicate.PREDTYPE.GEQ);
        Predicate p2_14 = new Predicate(schemaLineitem.getAttributeId("l_shipdate"), TypeUtils.TYPE.DATE, d14_2, Predicate.PREDTYPE.LT);
        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_14, p2_14});
        return q_l;
    }

    private static JoinQuery tpch19() {

        String brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1);
        String shipInstruct_19 = "DELIVER IN PERSON";
        double quantity_19 = rand.nextInt(10) + 1;
        Predicate p1_19 = new Predicate(schemaLineitem.getAttributeId("l_shipinstruct"), TypeUtils.TYPE.STRING, shipInstruct_19, Predicate.PREDTYPE.EQ);
        Predicate p4_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.GT);
        quantity_19 += 10;
        Predicate p5_19 = new Predicate(schemaLineitem.getAttributeId("l_quantity"), TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.LEQ);
        Predicate p8_19 = new Predicate(schemaLineitem.getAttributeId("l_shipmode"), TypeUtils.TYPE.STRING, "AIR", Predicate.PREDTYPE.LEQ);

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), new Predicate[]{p1_19, p4_19, p5_19, p8_19});

        return q_l;
    }


    public static Predicate getPredicate(Schema schema, String pred) {
        String[] parts = pred.split(" ");
        int attrId = schema.getAttributeId(parts[0].trim());

        if (attrId == -1) {
            throw new RuntimeException("Unknown attr: " + parts[0].trim());
        }

        TypeUtils.TYPE attrType = schema.getType(attrId);
        Object value = TypeUtils.deserializeValue(attrType, parts[2].trim().replaceAll("'", ""));
        String predTypeStr = parts[1].trim();
        Predicate.PREDTYPE predType;
        switch (predTypeStr) {
            case ">":
                predType = Predicate.PREDTYPE.GT;
                break;
            case ">=":
                predType = Predicate.PREDTYPE.GEQ;
                break;
            case "<":
                predType = Predicate.PREDTYPE.LT;
                break;
            case "<=":
                predType = Predicate.PREDTYPE.LEQ;
                break;
            case "=":
                predType = Predicate.PREDTYPE.EQ;
                break;
            default:
                throw new RuntimeException("Unknown predType " + predTypeStr);
        }

        Predicate p = new Predicate(attrId, attrType, value, predType);
        return p;
    }


    public static ArrayList<ArrayList<JoinQuery>> generateWorkload(ConfUtils cfg) {
        byte[] stringBytes = HDFSUtils.readFile(
                HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
                "/user/yilu/cmt_queries.log");

        String queriesString = new String(stringBytes);
        String[] queries = queriesString.split("\n");
        ArrayList<ArrayList<JoinQuery>> ret = new ArrayList<ArrayList<JoinQuery>>();
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];
            String[] predicates = query.split(";");
            ArrayList<Predicate> mhPreds = new ArrayList<Predicate>();
            ArrayList<Predicate> sfPreds = new ArrayList<Predicate>();

            ArrayList<JoinQuery> q = new ArrayList<JoinQuery>();

            for (int j = 0; j < predicates.length; j++) {
                if (predicates[j].startsWith(MH)) {
                    Predicate p = getPredicate(schemaMH, predicates[j]);
                    mhPreds.add(p);
                } else {
                    Predicate p = getPredicate(schemaSF, predicates[j]);
                    sfPreds.add(p);
                }
            }

            Predicate[] mhArray = mhPreds.toArray(new Predicate[mhPreds.size()]);
            Predicate[] sfArray = sfPreds.toArray(new Predicate[sfPreds.size()]);

            JoinQuery q_mh = new JoinQuery(MH, schemaMH.getAttributeId("mh_id"), mhArray);
            JoinQuery q_sf = new JoinQuery(SF, schemaSF.getAttributeId("sf_id"), sfArray);

            q.add(q_mh);
            q.add(q_sf);

            ret.add(q);
        }

        return ret;
    }


    public static void testTPCH() {
        setup();

        Predicate[] EmptyPredicates = {};

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        Globals.loadTableInfo(lineitem, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(lineitem);

        JoinRobustTree.randGenerator.setSeed(0);




        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_partkey"), EmptyPredicates);
        q_l.setForceRepartition(true);


        JoinOptimizer opt = new JoinOptimizer(cfg);
        opt.loadIndex(tableInfo);

        opt.checkNotEmpty(opt.getIndex().getRoot());

        opt.loadQueries(tableInfo);
        opt.buildPlan(q_l);


        for (int i = 0; i < 10; i++) {
            q_l = tpch14();
            opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());

            opt.loadQueries(tableInfo);
            opt.buildPlan(q_l);
        }

        for (int i = 0; i < 20; i++) {
            double p = (20 - i) / 20.0;
            if (rand.nextDouble() <= p) {
                q_l = tpch14();
                System.out.println("INFO: Running query TPC-H q14:" + q_l);
            } else {
                q_l = tpch19();
                System.out.println("INFO: Running query TPC-H q19:" + q_l);
            }
            opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());

            opt.loadQueries(tableInfo);
            opt.buildPlan(q_l);
        }

        for (int i = 0; i < 10; i++) {
            q_l = tpch19();
            opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());

            opt.loadQueries(tableInfo);
            opt.buildPlan(q_l);
        }

        for (int i = 0; i < 20; i++) {
            double p = (20 - i) / 20.0;
            if (rand.nextDouble() <= p) {
                q_l = tpch19();
                System.out.println("INFO: Running query TPC-H q19:" + q_l);
            } else {
                q_l = tpch14();
                System.out.println("INFO: Running query TPC-H q14:" + q_l);
            }
            opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());

            opt.loadQueries(tableInfo);
            opt.buildPlan(q_l);
        }


        for (int i = 0; i < 10; i++) {
            q_l = tpch14();
            opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());

            opt.loadQueries(tableInfo);
            opt.buildPlan(q_l);
        }

/*
        for (int i = 0; i < 60; i++) {
            if (i < 20) {
                double p = (20 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    q_l = tpch14();
                    System.out.println("INFO: Running query TPC-H q14:" + q_l);
                } else {
                    q_l = tpch19();
                    System.out.println("INFO: Running query TPC-H q19:" + q_l);
                }
            } else if (i < 40) {
                double p = (40 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    q_l = tpch19();
                    System.out.println("INFO: Running query TPC-H q19:" + q_l);
                } else {
                    q_l = tpch14();
                    System.out.println("INFO: Running query TPC-H q14:" + q_l);
                }
            } else if (i < 60){
                double p = (60 - i) / 20.0;
                if (rand.nextDouble() <= p) {
                    q_l = tpch14();
                    System.out.println("INFO: Running query TPC-H q14:" + q_l);
                } else {
                    q_l = tpch19();
                    System.out.println("INFO: Running query TPC-H q19:" + q_l);
                }
            }

            opt = new JoinOptimizer(cfg);
            opt.loadIndex(tableInfo);

            opt.checkNotEmpty(opt.getIndex().getRoot());

            opt.loadQueries(tableInfo);
            opt.buildPlan(q_l);
        }
 */
    }

    public static void testCMT() {
        setup();

        ConfUtils cfg = new ConfUtils("/Users/ylu/Documents/workspace/mdindex/conf/ylu.properties");
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        Globals.loadTableInfo(MH, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(MH);


        JoinRobustTree.randGenerator.setSeed(0);


        ArrayList<ArrayList<JoinQuery>> queries = generateWorkload(cfg);

        int iters = 0;

        RawIndexKey key = new RawIndexKey('|');
        byte[] sampleBytes = HDFSUtils.readFile(fs, "/user/yilu/mh/sample");

        String samples = new String(sampleBytes);
        String[] keys = samples.split("\n");

        for (ArrayList<JoinQuery> q : queries) {

            JoinQuery q_mh = q.get(0);

            System.out.println(q_mh);

            ++iters;

            if (iters == 1) {
                q_mh.setForceRepartition(true);
            }

            JoinOptimizer opt = new JoinOptimizer(cfg);

            opt.loadIndex(tableInfo);


            HashSet<Integer> bids = new HashSet<Integer>();

            for (int i = 0; i < keys.length; i++) {
                key.setBytes(keys[i].getBytes());
                int bid = (int) opt.getIndex().getBucketId(key);
                bids.add(bid);
                if (bid == 3520) {
                    System.out.println(keys[i]);
                }
            }
            int a = bids.size();

            System.out.println("NUM of data blocks: " + opt.getIndex().getAllBuckets().length);
            opt.checkNotEmpty(opt.getIndex().getRoot());
            opt.loadQueries(tableInfo);
            opt.buildPlan(q_mh);

            if (iters == 2) break;
        }

    }

    public static void main(String[] args) {
        testTPCH();
    }

}
