package perf.benchmark;

/**
 * Created by ylu on 4/25/16.
 */

import core.adapt.spark.RangePartitioner;
import core.adapt.spark.join.SparkJoinCopartitionedInputFormat;
import core.adapt.spark.join.SparkJoinQuery;
import core.adapt.spark.join.SparkJoinQueryConf;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.SparkUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class TPCHCopartitionedWorkload {


    public static class Mapper implements PairFunction<String, Long, String> {
        String Delimiter;
        String Splitter;
        int partitionKey;
        public Mapper(String Delimiter, int partitionKey) {
            this.Delimiter = Delimiter;
            this.partitionKey = partitionKey;
            if (Delimiter.equals("|"))
                Splitter = "\\|";
            else
                Splitter = Delimiter;
        }

        @Override
        public Tuple2<Long, String> call(String v1) throws Exception {
            long key = Long.parseLong(v1.split(Splitter)[partitionKey]);
            return new Tuple2<Long, String>(key, v1);
        }
    }

    private ConfUtils cfg;

    private Schema schemaLineitem, schemaOrders;
    private String stringLineitem, stringOrders;
    private TableInfo tableLineitem, tableOrders;

    private String lineitem = "lineitem", orders = "orders";
    private Predicate[] EmptyPredicates = {};

    private Random rand;
    private int method;

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);

        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);


        tableLineitem = new TableInfo(lineitem, 0, '|', schemaLineitem);
        tableOrders = new TableInfo(orders, 0, '|', schemaOrders);


    }

    public void garbageCollect() {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        tableLineitem.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tableOrders.gc(cfg.getHDFS_WORKING_DIR(), fs);

    }

    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
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
                case "--method":
                    method = Integer.parseInt(args[counter + 1]);
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

            fs.delete(new Path(path + "/_SUCCESS"), false);
            FileStatus[] fileStatus = fs.listStatus(new Path(path));

            for (int i = 0; i < fileStatus.length; i++) {
                String oldPath = fileStatus[i].getPath().toString();
                String baseName = FilenameUtils.getBaseName(oldPath);
                String dir = oldPath.substring(0, oldPath.length() - baseName.length());
                String newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));
                fs.rename(new Path(oldPath), new Path(newPath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void runCopartitionedWorkload() {
        SparkConf sconf = SparkUtils.getSparkConf(this.getClass().getName(), cfg);

        try {
            sconf.registerKryoClasses(new Class<?>[]{
                    Class.forName("org.apache.hadoop.io.LongWritable"),
                    Class.forName("org.apache.hadoop.io.Text")
            });
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        JavaSparkContext ctx = new JavaSparkContext(sconf);
        ctx.hadoopConfiguration().setBoolean(
                FileInputFormat.INPUT_DIR_RECURSIVE, true);
        ctx.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());


    
        JavaRDD<String> lineitem_text = ctx.textFile(cfg.getHDFS_WORKING_DIR() + "/lineitem/data");
        JavaRDD<String> orders_text = ctx.textFile(cfg.getHDFS_WORKING_DIR() + "/orders/data");


        JavaPairRDD<Long, String> lineitem_records = lineitem_text.mapToPair(new Mapper("|", schemaLineitem.getAttributeId("l_orderkey")));
        JavaPairRDD<Long, String> orders_records = orders_text.mapToPair(new Mapper("|", schemaOrders.getAttributeId("o_orderkey")));

        HashPartitioner partitioner = new HashPartitioner(200);


        JavaPairRDD<Long, String> lineitem_records_partitioned = lineitem_records.partitionBy(partitioner);
        JavaPairRDD<Long, String> orders_records_partitioned = orders_records.partitionBy(partitioner);


        lineitem_records_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/lineitem_co");
        orders_records_partitioned.values().saveAsTextFile(cfg.getHDFS_WORKING_DIR() + "/orders_co");


        cleanup(cfg.getHDFS_WORKING_DIR() + "/lineitem_co");
        cleanup(cfg.getHDFS_WORKING_DIR() + "/orders_co");

        Configuration conf = ctx.hadoopConfiguration();

        JoinQuery q_l = new JoinQuery(lineitem, schemaLineitem.getAttributeId("l_orderkey"), new Predicate[]{});
        JoinQuery q_o = new JoinQuery(orders, schemaOrders.getAttributeId("o_orderkey"), new Predicate[]{});

        conf.set("DATASET1", "lineitem_co");
        conf.set("DATASET2", "orders_co");

        conf.set("DATASET1_QUERY", q_l.toString());
        conf.set("DATASET2_QUERY", q_o.toString());

        conf.set("PARTITION_NUM", "200");
        conf.set("PARTITION_KEY", "0");

        conf.set("WORKING_DIR", cfg.getHDFS_WORKING_DIR());
        conf.set("HADOOP_HOME",cfg.getHADOOP_HOME());
        conf.set("ZOOKEEPER_HOSTS",cfg.getZOOKEEPER_HOSTS());
        conf.setInt("HDFS_REPLICATION_FACTOR",cfg.getHDFS_REPLICATION_FACTOR());
        conf.set("DELIMITER", "|");

        JavaPairRDD<LongWritable, Text> rdd = ctx.newAPIHadoopFile(cfg.getHADOOP_NAMENODE() + cfg.getHDFS_WORKING_DIR(),
                SparkJoinCopartitionedInputFormat.class, LongWritable.class,
                Text.class, ctx.hadoopConfiguration());

        long start = System.currentTimeMillis();

        long result = rdd.count();

        System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

    }


    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        TPCHCopartitionedWorkload t = new TPCHCopartitionedWorkload();
        t.loadSettings(args);
        t.setUp();


        switch (t.method) {
            case 1:
                t.runCopartitionedWorkload();
                break;
            default:
                break;
        }

        //t.garbageCollect();
    }
}
