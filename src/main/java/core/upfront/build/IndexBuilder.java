package core.upfront.build;

import core.common.index.JoinRobustTree;
import core.common.index.MDIndex;
import core.common.index.RobustTree;
import core.common.key.ParsedTupleList;
import core.common.key.RawIndexKey;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;


public class IndexBuilder {

    int bucketSize = 64 << 20; // 64 MB.

	/*
    public void sparkSampleInput(double sampleRate, String inputDirectory, String outputSamplePath,
								 FileSystem fs, ConfUtils cfg){

		SparkConf sconf = SparkUtils.getSparkConf(this.getClass().getName(), cfg);
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		ctx.hadoopConfiguration().setBoolean(
				FileInputFormat.INPUT_DIR_RECURSIVE, true);
		ctx.hadoopConfiguration().set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		long startTime = System.nanoTime();

		JavaRDD<String> input = ctx.textFile(inputDirectory);
		JavaRDD<String> sample = input.sample(false, sampleRate);

		String sparkTempOutput = outputSamplePath + ".temp";
		sample.saveAsTextFile(sparkTempOutput);

		//Merge files from sparkTempOutput into a single sample file.
		try {
			FileUtil.copyMerge(fs, new Path(sparkTempOutput),
					fs, new Path(outputSamplePath), true,
					ctx.hadoopConfiguration(), null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		double timeScanAndWrite = (System.nanoTime() - startTime) / 1e9;
		System.out.println("Scanning, sampling and write time: " + timeScanAndWrite + " sec");
	}
	*/

    // Reads the files from inputDirectory and samples them using blockSampling.
    // Writes out the samples to SAMPLES_DIR/sample.machineId.
    public void blockSampleInput(double samplingRate, RawIndexKey key, String inputDirectory,
                                 String outputSamplePath, FileSystem fs) {
        InputReader r = new InputReader(null, key);
        File[] files = new File(inputDirectory).listFiles();
        OutputStream out = HDFSUtils.getHDFSOutputStream(fs, outputSamplePath, (short) 1, 50 << 20);

        long startTime = System.nanoTime();
        try {
            for (File f : files) {
                System.out.println("Scanning " + f.getName());
                r.scanWithBlockSampling(f.getPath(), samplingRate, out);
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        double timeScanAndWrite = (System.nanoTime() - startTime) / 1e9;
        System.out.println("Scanning, sampling and write time: " + timeScanAndWrite + " sec");
    }

    // Build index from the samples collected.
    public void buildIndexFromSample(ParsedTupleList sample, int numBuckets, MDIndex index, PartitionWriter writer) {
        index.setMaxBuckets(numBuckets);
        ((RobustTree) index).loadSample(sample);

        long startTime = System.nanoTime();
        index.initProbe();
        System.out.println("BUILD: index building time = " + ((System.nanoTime() - startTime) / 1E9));

        startTime = System.nanoTime();
        byte[] indexBytes = index.marshall();
        System.out.println("Index Size: " + indexBytes.length);
        writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
        writer.flush();
        System.out.println("BUILD: index writing time = " + ((System.nanoTime() - startTime) / 1E9));
    }

    // Build join index from the samples collected.
    public void buildJoinIndexFromSample(ParsedTupleList sample, int numBuckets, int joinAttribute, MDIndex index, PartitionWriter writer) {
        index.setMaxBuckets(numBuckets);
        ((JoinRobustTree) index).loadSample(sample);

        long startTime = System.nanoTime();
        index.initProbe(joinAttribute);
        System.out.println("BUILD: index building time = " + ((System.nanoTime() - startTime) / 1E9));

        startTime = System.nanoTime();
        byte[] indexBytes = index.marshall();
        System.out.println("Index Size: " + indexBytes.length);
        writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
        writer.flush();
        System.out.println("BUILD: index writing time = " + ((System.nanoTime() - startTime) / 1E9));
    }

    // Given an index that has already been constructed, and a directory of
    // files to partition, writes out the appropriate partition files.
    public void buildDistributedFromIndex(MDIndex index, RawIndexKey key, String inputDirectory,
                                          PartitionWriter writer) {
        InputReader r = new InputReader(index, key);
        r.firstPass = false;
        File[] files = new File(inputDirectory).listFiles();

        long startTime = System.nanoTime();
        for (File f : files) {
            r.scan(f.getPath(), writer);
        }
        writer.flush();
        double time3 = (System.nanoTime() - startTime) / 1E9;
        System.out.println("Index Probe Time = " + time3 + " sec");
    }
}
