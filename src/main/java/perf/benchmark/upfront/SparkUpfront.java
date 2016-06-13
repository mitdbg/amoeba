package perf.benchmark.upfront;

import core.adapt.Query;
import core.utils.ConfUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import perf.benchmark.BenchmarkSettings;
import perf.benchmark.TPCHWorkload;

/**
 * Created by anil on 5/12/16.
 */
public class SparkUpfront {
    public String dropTpch = "DROP TABLE IF EXISTS tpch";
    public String createTpch = "CREATE TEMPORARY TABLE tpch "
            + "(l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,"
            + "l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,"
            + "l_shipinstruct string, l_shipmode string,"
            + "o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,"
            + "o_shippriority int,"
            + "p_name string, p_mfgr string, p_brand string,"
            + "p_type string, p_size int, p_container string, p_retailprice double,"
            + "s_name string, s_address string,"
            + "s_phone string, s_acctbal double, s_nation string, s_region string,"
            + "c_name string, c_address string,"
            + "c_phone string, c_acctbal double, c_mktsegment string , c_nation string, c_region string) "
            + "USING com.databricks.spark.csv "
            + "OPTIONS (path \"/user/amoeba/uploadtest/\", header \"false\", delimiter \"|\")";

    private ConfUtils cfg;

    private JavaSparkContext ctx;
    private SQLContext sqlContext;
    // SparkContext sc;

    public SparkUpfront() {

    }

    public static void main(String[] args) {
        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();
        SparkUpfront s = new SparkUpfront();
        s.setUp();
        s.runQueries();
    }

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);

        SparkConf sconf = new SparkConf().setMaster(cfg.getSPARK_MASTER())
                .setAppName("Spark App")
                .setSparkHome(cfg.getSPARK_HOME())
                .setJars(new String[]{cfg.getSPARK_APPLICATION_JAR()})
                .set("spark.hadoop.cloneConf", "false")
                .set("spark.executor.memory", cfg.getSPARK_EXECUTOR_MEMORY())
                .set("spark.driver.memory", cfg.getSPARK_DRIVER_MEMORY())
                .set("spark.task.cpus", "1");

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
        sqlContext.sql(createTpch);
    }

    public void runQueries() {
        TPCHWorkload w = new TPCHWorkload();
        w.setUp();

        for (int i = 0; i < w.supportedQueries.length; i++) {
            Query q = w.getQuery(w.supportedQueries[i]);
            long start = System.currentTimeMillis();
            String query = q.createQueryString();
            System.out.println("Query:" + query);
            DataFrame df = sqlContext.sql(query);
            long result = df.count();
            long end = System.currentTimeMillis();
            System.out.println("RES: Query: " + w.supportedQueries[i] + ";Time Taken: " + (end - start) +
                    "; Result: " + result);
        }
    }
}
