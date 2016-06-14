package core.upfront.build;

import core.common.index.MDIndex;
import core.common.key.RawIndexKey;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import perf.benchmark.BenchmarkSettings;
import scala.Tuple2;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

/**
 * Created by anil on 6/13/16.
 */
public class SparkDataUploader {
    // Given an index that has already been constructed, and a directory of
    // files to partition, writes out the appropriate partition files.
    public static void buildDistributedFromIndex(final MDIndex index, final RawIndexKey key, String inputDirectory,
                                                 final String tableHDFSDir, final ConfUtils cfg) {
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
                    Class.forName("org.apache.hadoop.io.Text"),
            });
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        JavaSparkContext ctx = new JavaSparkContext(sconf);
        ctx.hadoopConfiguration().setBoolean(
                FileInputFormat.INPUT_DIR_RECURSIVE, true);
        ctx.hadoopConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        final String hadoopHome = cfg.getHADOOP_HOME();
        final String outputDirectory = tableHDFSDir + "/data/";
        final short replication = cfg.getHDFS_REPLICATION_FACTOR();
        final RawIndexKey tuple = new RawIndexKey(key.getDelimiter());

        JavaRDD<String> inputData = ctx.textFile(inputDirectory);
        JavaPairRDD<Integer, String> mappedData = inputData.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                tuple.setBytes(s.getBytes());
                int bid = index.getBucketId(tuple);
                return new Tuple2(bid, s);
            }
        });

        JavaPairRDD<Integer, Iterable<String>> bucketedData = mappedData.groupByKey();
        bucketedData.foreach(new VoidFunction<Tuple2<Integer, Iterable<String>>>() {
            @Override
            public void call(Tuple2<Integer, Iterable<String>> bucket) throws Exception {
                FileSystem fs = HDFSUtils.getFSByHadoopHome(hadoopHome);
                int bid = bucket._1();
                String filename = outputDirectory + bid;
                Path fPath = new Path(filename);
                if (fs.exists(fPath)) {
                    fs.delete(fPath, false);
                }
                OutputStream os = new BufferedOutputStream(fs.create(fPath, replication), 1024*1024);
                for (String tuple: bucket._2()) {
                    os.write(tuple.getBytes());
                    // TODO: Maybe missing "\n"
                }
                os.close();
            }
        });
    }
}
