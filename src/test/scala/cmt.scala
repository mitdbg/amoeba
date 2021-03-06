// Load CMT tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://128.30.77.86:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --driver-memory 4G --executor-memory 100G -i ~/cmt.scala
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.SaveMode

val PATH = "hdfs://istc13.csail.mit.edu:9000/user/mdindex/cmt100-raw"
val DEST = "hdfs://istc13.csail.mit.edu:9000/user/mdindex/cmt100"

sqlContext.sql(s"""CREATE TEMPORARY TABLE mapmatch_history (mh_id int, mh_dataset_id string, 
		mh_uploadtime string, mh_runtime string, 
		mh_trip_start string, mh_data_count_minutes string, 
		mh_data_count_accel_samples string, mh_data_count_netloc_samples string, 
		mh_data_count_gps_samples string, mh_observed_sample_rate string, 
		mh_distance_mapmatched_km string, mh_distance_gps_km string, 
		mh_ground_truth_present string, mh_timing_mapmatch string, 
		mh_distance_pct_path_error string, mh_build_version string, 
		mh_timing_queue_wait string, mh_data_trip_length string, 
		mh_battery_maximum_level string, mh_battery_minimum_level string, 
		mh_battery_drain_rate_per_hour string, mh_battery_plugged_duration_hours string, 
		mh_battery_delay_from_drive_end_seconds string, mh_startlat string, 
		mh_startlon string, mh_endlat string, 
		mh_endlon string, mh_data_count_output_gps_speeding_points string, 
		mh_speeding_slow_gps_points string, mh_speeding_10kmh_gps_points string, 
		mh_speeding_20kmh_gps_points string, mh_speeding_40kmh_gps_points string, 
		mh_speeding_80kmh_gps_points string, mh_output_accel_valid_minutes string, 
		mh_output_gps_moving_minutes string, mh_output_gps_moving_and_accel_valid_minutes string, 
		mh_data_time_till_first_gps_minutes string, mh_score_di_accel string, 
		mh_score_di_brake string, mh_score_di_turn string, 
		mh_score_di_car_motion string, mh_score_di_phone_motion string, 
		mh_score_di_speeding string, mh_score_di_night string, 
		mh_star_rating string, mh_trip_end string, 
		mh_score_di_car_motion_with_accel string, mh_score_di_car_motion_with_speeding string, 
		mh_score_di_distance_km_with_accel string, mh_score_di_distance_km_with_speeding string, 
		mh_score_accel_per_sec_ntile string, mh_score_brake_per_sec_ntile string, 
		mh_score_turn_per_sec_ntile string, mh_score_speeding_per_sec_ntile string, 
		mh_score_phone_motion_per_sec_ntile string, mh_score_accel_per_km_ntile string, 
		mh_score_brake_per_km_ntile string, mh_score_turn_per_km_ntile string, 
		mh_score_speeding_per_km_ntile string, mh_score_phone_motion_per_km_ntile string, 
		mh_score string, mh_distance_prepended_km string, 
		mh_recording_start string, mh_score_di_distance_km string, 
		mh_recording_end string, mh_recording_startlat string, 
		mh_recording_startlon string, mh_display_distance_km string, 
		mh_display_trip_start string, mh_display_startlat string, 
		mh_display_startlon string, mh_data_count_gyro_samples string, 
		mh_star_rating_accel string, mh_star_rating_brake string, 
		mh_star_rating_turn string, mh_star_rating_speeding string, 
		mh_star_rating_phone_motion string, mh_is_night string, 
		mh_battery_total_drain string, mh_battery_total_drain_duration_hours string, 
		mh_score_smoothness string, mh_score_awareness string, 
		mh_star_rating_night string, mh_star_rating_smoothness string, 
		mh_star_rating_awareness string, mh_hide string, 
		mh_data_count_tag_accel_samples string, mh_quat_i string, 
		mh_quat_j string, mh_quat_k string, 
		mh_quat_r string, mh_passenger_star_rating string, 
		mh_suspension_damping_ratio string, mh_suspension_natural_frequency string, 
		mh_suspension_fit_error string, mh_driving string, 
		mh_trip_mode string, mh_classification_confidence string, 
		mh_gk_trip_mode string, mh_gk_confidence string, 
		mh_offroad_trip_mode string, mh_offroad_confidence string, 
		mh_driver_confidence string, mh_timing_processing_preprocessing string, 
		mh_timing_processing_gatekeeper string, mh_timing_processing_accelpipeline string, 
		mh_timing_processing_offroad string, mh_timing_processing_suspension string, 
		mh_timing_processing_scoring string, mh_timing_processing_hitchhiker string, 
		mh_data_count_obd_samples string, mh_data_count_pressure_samples string, 
		mh_raw_sampling_mode string, mh_data_count_magnetometer_samples string, 
		mh_location_disabled_date string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/mapmatch_history.txt.1,$PATH/mapmatch_history.txt.2,$PATH/mapmatch_history.txt.3,$PATH/mapmatch_history.txt.4,$PATH/mapmatch_history.txt.5,$PATH/mapmatch_history.txt.6,$PATH/mapmatch_history.txt.7,$PATH/mapmatch_history.txt.8,$PATH/mapmatch_history.txt.9,$PATH/mapmatch_history.txt.0", header "false", delimiter "#", comment "~")""")

sqlContext.sql(s"""CREATE TEMPORARY TABLE mapmatch_history_latest (mhl_dataset_id int, mhl_mapmatch_history_id int
  )
USING com.databricks.spark.csv
OPTIONS (path "$PATH/mapmatch_history_latest.txt.1,$PATH/mapmatch_history_latest.txt.2,$PATH/mapmatch_history_latest.txt.3,$PATH/mapmatch_history_latest.txt.4,$PATH/mapmatch_history_latest.txt.5,$PATH/mapmatch_history_latest.txt.6,$PATH/mapmatch_history_latest.txt.7,$PATH/mapmatch_history_latest.txt.8,$PATH/mapmatch_history_latest.txt.9,$PATH/mapmatch_history_latest.txt.0", header "false", delimiter "#", comment "~")""")

sqlContext.sql(s"""CREATE TEMPORARY TABLE sf_datasets (sf_id int, sf_uploadtime string, 
		sf_deviceid string, sf_driveid string, 
		sf_state string, sf_dest_server string, 
		sf_companyid string, sf_hardware_manufacturer string, 
		sf_hardware_model string, sf_hardware_bootloader string, 
		sf_hardware_build string, sf_hardware_carrier string, 
		sf_android_fw_version string, sf_android_api_version string, 
		sf_android_codename string, sf_android_baseband string, 
		sf_raw_hardware_string string, sf_raw_os_string string, 
		sf_utc_offset_with_dst string, sf_app_version string, 
		sf_file_format string, sf_start_reason string, 
		sf_stop_reason string, sf_previous_driveid string, 
		sf_userid string, sf_tag_mac_address string, 
		sf_tag_trip_number string, sf_primary_driver_app_user_id string, 
		sf_tag_last_connection_number string, sf_gps_points_lsh_key_1 string, 
		sf_gps_points_lsh_key_2 string, sf_gps_points_lsh_key_3 string, 
		sf_hidden_by_support string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/sf_datasets.txt.1,$PATH/sf_datasets.txt.2,$PATH/sf_datasets.txt.3,$PATH/sf_datasets.txt.4,$PATH/sf_datasets.txt.5,$PATH/sf_datasets.txt.6,$PATH/sf_datasets.txt.7,$PATH/sf_datasets.txt.8,$PATH/sf_datasets.txt.9,$PATH/sf_datasets.txt.0", header "false", delimiter "#", comment "~")""")

val table = sqlContext.sql(s"""SELECT mh_id, 
		mh_dataset_id, mh_uploadtime, 
		mh_runtime, mh_trip_start, 
		mh_data_count_minutes, mh_data_count_accel_samples, 
		mh_data_count_netloc_samples, mh_data_count_gps_samples, 
		mh_observed_sample_rate, mh_distance_mapmatched_km, 
		mh_distance_gps_km, mh_ground_truth_present, 
		mh_timing_mapmatch, mh_distance_pct_path_error, 
		mh_build_version, mh_timing_queue_wait, 
		mh_data_trip_length, mh_battery_maximum_level, 
		mh_battery_minimum_level, mh_battery_drain_rate_per_hour, 
		mh_battery_plugged_duration_hours, mh_battery_delay_from_drive_end_seconds, 
		mh_startlat, mh_startlon, 
		mh_endlat, mh_endlon, 
		mh_data_count_output_gps_speeding_points, mh_speeding_slow_gps_points, 
		mh_speeding_10kmh_gps_points, mh_speeding_20kmh_gps_points, 
		mh_speeding_40kmh_gps_points, mh_speeding_80kmh_gps_points, 
		mh_output_accel_valid_minutes, mh_output_gps_moving_minutes, 
		mh_output_gps_moving_and_accel_valid_minutes, mh_data_time_till_first_gps_minutes, 
		mh_score_di_accel, mh_score_di_brake, 
		mh_score_di_turn, mh_score_di_car_motion, 
		mh_score_di_phone_motion, mh_score_di_speeding, 
		mh_score_di_night, mh_star_rating, 
		mh_trip_end, mh_score_di_car_motion_with_accel, 
		mh_score_di_car_motion_with_speeding, mh_score_di_distance_km_with_accel, 
		mh_score_di_distance_km_with_speeding, mh_score_accel_per_sec_ntile, 
		mh_score_brake_per_sec_ntile, mh_score_turn_per_sec_ntile, 
		mh_score_speeding_per_sec_ntile, mh_score_phone_motion_per_sec_ntile, 
		mh_score_accel_per_km_ntile, mh_score_brake_per_km_ntile, 
		mh_score_turn_per_km_ntile, mh_score_speeding_per_km_ntile, 
		mh_score_phone_motion_per_km_ntile, mh_score, 
		mh_distance_prepended_km, mh_recording_start, 
		mh_score_di_distance_km, mh_recording_end, 
		mh_recording_startlat, mh_recording_startlon, 
		mh_display_distance_km, mh_display_trip_start, 
		mh_display_startlat, mh_display_startlon, 
		mh_data_count_gyro_samples, mh_star_rating_accel, 
		mh_star_rating_brake, mh_star_rating_turn, 
		mh_star_rating_speeding, mh_star_rating_phone_motion, 
		mh_is_night, mh_battery_total_drain, 
		mh_battery_total_drain_duration_hours, mh_score_smoothness, 
		mh_score_awareness, mh_star_rating_night, 
		mh_star_rating_smoothness, mh_star_rating_awareness, 
		mh_hide, mh_data_count_tag_accel_samples, 
		mh_quat_i, mh_quat_j, 
		mh_quat_k, mh_quat_r, 
		mh_passenger_star_rating, mh_suspension_damping_ratio, 
		mh_suspension_natural_frequency, mh_suspension_fit_error, 
		mh_driving, mh_trip_mode, 
		mh_classification_confidence, mh_gk_trip_mode, 
		mh_gk_confidence, mh_offroad_trip_mode, 
		mh_offroad_confidence, mh_driver_confidence, 
		mh_timing_processing_preprocessing, mh_timing_processing_gatekeeper, 
		mh_timing_processing_accelpipeline, mh_timing_processing_offroad, 
		mh_timing_processing_suspension, mh_timing_processing_scoring, 
		mh_timing_processing_hitchhiker, mh_data_count_obd_samples, 
		mh_data_count_pressure_samples, mh_raw_sampling_mode, 
		mh_data_count_magnetometer_samples, mh_location_disabled_date, 
		sf_id, sf_uploadtime, 
		sf_deviceid, sf_driveid, 
		sf_state, sf_dest_server, 
		sf_companyid, sf_hardware_manufacturer, 
		sf_hardware_model, sf_hardware_bootloader, 
		sf_hardware_build, sf_hardware_carrier, 
		sf_android_fw_version, sf_android_api_version, 
		sf_android_codename, sf_android_baseband, 
		sf_raw_hardware_string, sf_raw_os_string, 
		sf_utc_offset_with_dst, sf_app_version, 
		sf_file_format, sf_start_reason, 
		sf_stop_reason, sf_previous_driveid, 
		sf_userid, sf_tag_mac_address, 
		sf_tag_trip_number, sf_primary_driver_app_user_id, 
		sf_tag_last_connection_number, sf_gps_points_lsh_key_1, 
		sf_gps_points_lsh_key_2, sf_gps_points_lsh_key_3, 
		sf_hidden_by_support
FROM mapmatch_history
JOIN mapmatch_history_latest ON mapmatch_history.mh_id = mapmatch_history_latest.mhl_mapmatch_history_id
JOIN sf_datasets ON mapmatch_history_latest.mhl_dataset_id = sf_datasets.sf_id """)

table.registerTempTable("table")

table.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> DEST, "delimiter" -> "|"))
