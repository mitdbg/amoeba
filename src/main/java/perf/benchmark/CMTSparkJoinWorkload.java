package perf.benchmark;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * Created by ylu on 1/5/16.
 */


public class CMTSparkJoinWorkload {

    private ConfUtils cfg;

    private String MH = "mh", MHL = "mhl", SF = "sf";

    private int method;

    private int numQueries;

    private Random rand;

    private JavaSparkContext ctx;
    private SQLContext sqlContext;

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        CMTSparkJoinWorkload t = new CMTSparkJoinWorkload();
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


        // Create MH table
        String mhPath = cfg.getHDFS_WORKING_DIR() + "/" + MH + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE mapmatch_history (mh_id int, mh_dataset_id string, "
                + "mh_uploadtime string, mh_runtime string, "
                + "mh_trip_start string, mh_data_count_minutes string, "
                + "mh_data_count_accel_samples string, mh_data_count_netloc_samples string, "
                + "mh_data_count_gps_samples string, mh_observed_sample_rate string, "
                + "mh_distance_mapmatched_km string, mh_distance_gps_km string, "
                + "mh_ground_truth_present string, mh_timing_mapmatch string, "
                + "mh_distance_pct_path_error string, mh_build_version string, "
                + "mh_timing_queue_wait string, mh_data_trip_length string, "
                + "mh_battery_maximum_level string, mh_battery_minimum_level string, "
                + "mh_battery_drain_rate_per_hour string, mh_battery_plugged_duration_hours string, "
                + "mh_battery_delay_from_drive_end_seconds string, mh_startlat string, "
                + "mh_startlon string, mh_endlat string, "
                + "mh_endlon string, mh_data_count_output_gps_speeding_points string, "
                + "mh_speeding_slow_gps_points string, mh_speeding_10kmh_gps_points string, "
                + "mh_speeding_20kmh_gps_points string, mh_speeding_40kmh_gps_points string, "
                + "mh_speeding_80kmh_gps_points string, mh_output_accel_valid_minutes string, "
                + "mh_output_gps_moving_minutes string, mh_output_gps_moving_and_accel_valid_minutes string, "
                + "mh_data_time_till_first_gps_minutes string, mh_score_di_accel string, "
                + "mh_score_di_brake string, mh_score_di_turn string, "
                + "mh_score_di_car_motion string, mh_score_di_phone_motion string, "
                + "mh_score_di_speeding string, mh_score_di_night string, "
                + "mh_star_rating string, mh_trip_end string, "
                + "mh_score_di_car_motion_with_accel string, mh_score_di_car_motion_with_speeding string, "
                + "mh_score_di_distance_km_with_accel string, mh_score_di_distance_km_with_speeding string, "
                + "mh_score_accel_per_sec_ntile string, mh_score_brake_per_sec_ntile string, "
                + " mh_score_turn_per_sec_ntile string, mh_score_speeding_per_sec_ntile string, "
                + "mh_score_phone_motion_per_sec_ntile string, mh_score_accel_per_km_ntile string, "
                + "mh_score_brake_per_km_ntile string, mh_score_turn_per_km_ntile string, "
                + "mh_score_speeding_per_km_ntile string, mh_score_phone_motion_per_km_ntile string, "
                + "mh_score string, mh_distance_prepended_km string, "
                + "mh_recording_start string, mh_score_di_distance_km string, "
                + "mh_recording_end string, mh_recording_startlat string, "
                + "mh_recording_startlon string, mh_display_distance_km string, "
                + "mh_display_trip_start string, mh_display_startlat string, "
                + "mh_display_startlon string, mh_data_count_gyro_samples string, "
                + "mh_star_rating_accel string, mh_star_rating_brake string, "
                + "mh_star_rating_turn string, mh_star_rating_speeding string, "
                + "mh_star_rating_phone_motion string, mh_is_night string, "
                + "mh_battery_total_drain string, mh_battery_total_drain_duration_hours string, "
                + "mh_score_smoothness string, mh_score_awareness string, "
                + "mh_star_rating_night string, mh_star_rating_smoothness string, "
                + "mh_star_rating_awareness string, mh_hide string, "
                + "mh_data_count_tag_accel_samples string, mh_quat_i string, "
                + "mh_quat_j string, mh_quat_k string, "
                + "mh_quat_r string, mh_passenger_star_rating string, "
                + "mh_suspension_damping_ratio string, mh_suspension_natural_frequency string, "
                + "mh_suspension_fit_error string, mh_driving string, "
                + "mh_trip_mode string, mh_classification_confidence string, "
                + "mh_gk_trip_mode string, mh_gk_confidence string, "
                + "mh_offroad_trip_mode string, mh_offroad_confidence string, "
                + "mh_driver_confidence string, mh_timing_processing_preprocessing string, "
                + "mh_timing_processing_gatekeeper string, mh_timing_processing_accelpipeline string, "
                + "mh_timing_processing_offroad string, mh_timing_processing_suspension string, "
                + "mh_timing_processing_scoring string, mh_timing_processing_hitchhiker string, "
                + "mh_data_count_obd_samples string, mh_data_count_pressure_samples string, "
                + "mh_raw_sampling_mode string, mh_data_count_magnetometer_samples string, "
                + " mh_location_disabled_date string) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + mhPath + "\", header \"false\", delimiter \"|\")");

        // Create MHL table
        String mhlPath = cfg.getHDFS_WORKING_DIR() + "/" + MHL + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE mapmatch_history_latest (mhl_dataset_id int, "
                + "mhl_mapmatch_history_id int) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + mhlPath + "\", header \"false\", delimiter \"|\")");

        // Create SF table
        String sfPath = cfg.getHDFS_WORKING_DIR() + "/" + SF + "/data";

        sqlContext.sql("CREATE TEMPORARY TABLE sf_datasets (sf_id int, sf_uploadtime string, "
                + "sf_deviceid string, sf_driveid string, "
                + "sf_state string, sf_dest_server string, "
                + "sf_companyid string, sf_hardware_manufacturer string, "
                + "sf_hardware_model string, sf_hardware_bootloader string, "
                + "sf_hardware_build string, sf_hardware_carrier string, "
                + "sf_android_fw_version string, sf_android_api_version string, "
                + "sf_android_codename string, sf_android_baseband string, "
                + " sf_raw_hardware_string string, sf_raw_os_string string, "
                + " sf_utc_offset_with_dst string, sf_app_version string, "
                + " sf_file_format string, sf_start_reason string, "
                + "sf_stop_reason string, sf_previous_driveid string, "
                + "sf_userid string, sf_tag_mac_address string, "
                + "sf_tag_trip_number string, sf_primary_driver_app_user_id string, "
                + "sf_tag_last_connection_number string, sf_gps_points_lsh_key_1 string, "
                + " sf_gps_points_lsh_key_2 string, sf_gps_points_lsh_key_3 string, "
                + " sf_hidden_by_support string) "
                + "USING com.databricks.spark.csv "
                + "OPTIONS (path \"" + sfPath + "\", header \"false\", delimiter \"|\")");

    }

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

    public ArrayList<String> generateWorkload() {
        byte[] stringBytes = HDFSUtils.readFile(
                HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
                "/user/yilu/cmt100000000/cmt_queries.log");
        String queriesString = new String(stringBytes);
        String[] queries = queriesString.split("\n");
        ArrayList<String> ret = new ArrayList<String>();
        for (int i = 0; i < queries.length; i++) {
            String q = queries[i].replaceAll(";", " and ");
            ret.add(q);
        }

        return ret;
    }

    // sf ⋈ (mhl ⋈ mh)
    public void runWorkload() {

        int cnt = 0;

        List<String> queries = generateWorkload();

        for (String q : queries) {

            long start = System.currentTimeMillis();

            System.out.println("SELECT  COUNT(*) "
                    + "FROM mapmatch_history JOIN mapmatch_history_latest ON mh_id = mhl_mapmatch_history_id "
                    + "JOIN sf_datasets ON sf_id = mhl_dataset_id "
                    + "WHERE " + q);

            DataFrame df = sqlContext.sql("SELECT  COUNT(*) "
                    + "FROM mapmatch_history JOIN mapmatch_history_latest ON mh_id = mhl_mapmatch_history_id "
                    + "JOIN sf_datasets ON sf_id = mhl_dataset_id "
                    + "WHERE " + q);

            String result = df.collect()[0].toString();
            System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

        }
    }
}
