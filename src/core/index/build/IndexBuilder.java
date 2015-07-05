package core.index.build;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import core.index.MDIndex;
import core.access.spark.SparkUpfrontPartitioner;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RobustTreeHs;

public class IndexBuilder {

	int bucketSize = 64*1024*1024;

	// Inserts all records from the input file into the index, builds the index, and writes the partitions.
	// Also writes the index and sample.
	public void build(MDIndex index, CartilageIndexKey key, String inputFilename, PartitionWriter writer){

		long startTime = System.nanoTime();
		File f = new File(inputFilename);
		long fileSize = f.length();
		index.initBuild((int) (fileSize / bucketSize) + 1);
		InputReader r = new InputReader(index, key);
		r.scan(inputFilename);
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scanning and sampling time = "+time1+" sec");

		startTime = System.nanoTime();
		index.initProbe();
		double time2 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Build Time = "+time2+" sec");

		startTime = System.nanoTime();
		r.scan(inputFilename, writer);
		writer.flush();
		double time3 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = "+time3+" sec");

		startTime = System.nanoTime();
		byte[] indexBytes = index.marshall();
		writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
		writer.flush();

		byte[] sampleBytes = ((RobustTreeHs)index).serializeSample();
		writer.writeToPartition("sample", sampleBytes, 0, sampleBytes.length);
		writer.flush();

		double time4 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index+Sample Write Time = "+time4+" sec");

		System.out.println("Total time = "+(time1+time2+time3+time4)+" sec");
	}

	// Samples records from the input file using block sampling, inserts those records into the index,
	// builds the index, and writes the partitions. Also writes the index and sample.
	public void buildWithBlockSampling(double samplingRate, MDIndex index, CartilageIndexKey key, String inputFilename, PartitionWriter writer){

		long startTime = System.nanoTime();
		File f = new File(inputFilename);
		long fileSize = f.length();
		index.initBuild((int) (fileSize / bucketSize) + 1);
		InputReader r = new InputReader(index, key);
		r.scanWithBlockSampling(inputFilename, samplingRate);
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scanning and sampling time = "+time1 + " sec");

		startTime = System.nanoTime();
		index.initProbe();
		double time2 = (System.nanoTime() - startTime)/1E9;
		System.out.println("Index Build Time = " + time2 + " sec");

		startTime = System.nanoTime();
		r.scan(inputFilename, writer);
		writer.flush();
		double time3 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = "+time3+" sec");

		startTime = System.nanoTime();
		byte[] indexBytes = index.marshall();
		writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
		writer.flush();

		byte[] sampleBytes = ((RobustTreeHs)index).serializeSample();
		writer.writeToPartition("sample", sampleBytes, 0, sampleBytes.length);
		writer.flush();

		double time4 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index+Sample Write Time = "+time4+" sec");

		System.out.println("Total time = "+(time1+time2+time3+time4)+" sec");
	}

	// Building index with block sampling, but files to sample are in a directory, rather than just a file
	// takes a precomputed number of buckets (since we don't know how big the files are in advance)
	// Doesn't write out the sample or index to HDFS
	public void buildWithBlockSamplingDir(double samplingRate, int bucketNum, MDIndex index, CartilageIndexKey key, String inputDirectory) {
		InputReader r = new InputReader(index, key);
		index.initBuild(bucketNum);
		System.out.println(inputDirectory);
		File[] files = new File(inputDirectory).listFiles();

		long startTime = System.nanoTime();
		for (File f : files) {
			r.scanWithBlockSampling(f.getPath(), samplingRate);
			r.firstPass = true;
		}
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scanning and sampling time = "+time1+" sec");

		startTime = System.nanoTime();
		index.initProbe();
		double time2 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Build Time = " + time2 + " sec");
	}

	// Doesn't really work (deprecated)
	public void buildWithSpark(double samplingRate, RobustTreeHs index, CartilageIndexKey key, String inputFilename, PartitionWriter writer, String propertiesFile, String hdfsPath){

		long startTime = System.nanoTime();
		File f = new File(inputFilename);
		long fileSize = f.length();
		index.initBuild((int) (fileSize / bucketSize) + 1);
		InputReader r = new InputReader(index, key);
		r.scanWithBlockSampling(inputFilename, samplingRate);
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scanning and sampling time = "+time1+" sec");

		startTime = System.nanoTime();
		index.initProbe();
		double time2 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Build Time = " + time2 + " sec");

		startTime = System.nanoTime();
		SparkUpfrontPartitioner partitioner = new SparkUpfrontPartitioner(propertiesFile, hdfsPath);
		partitioner.createTextFile(inputFilename, index.getRoot());
		double time3 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = "+time3+" sec");

		startTime = System.nanoTime();
		byte[] indexBytes = index.marshall();
		writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
		writer.flush();

		byte[] sampleBytes = index.serializeSample();
		writer.writeToPartition("sample", sampleBytes, 0, sampleBytes.length);
		writer.flush();

		double time4 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index+Sample Write Time = "+time4+" sec");

		System.out.println("Total time = "+(time1+time2+time3+time4)+" sec");
	}

	// Given an index that has already been constructed, and a directory of files to partition,
	// writes out the appropriate partition files
	public void buildDistributedFromIndex(MDIndex index, CartilageIndexKey key, String inputDirectory, PartitionWriter writer){

		InputReader r = new InputReader(index, key);
		r.firstPass = false;
		File[] files = new File(inputDirectory).listFiles();

		long startTime = System.nanoTime();
		for (File f : files) {
			r.scan(f.getPath(), writer);
			writer.flush();
		}
		writer.flush();
		double time3 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = " + time3 + " sec");
	}

	// Given an index that has already been constructed, and a directory of files to partition,
	// writes out the appropriate partition files
	// Multiple index version
	public void buildDistributedReplicasFromIndex(MDIndex[] indexes, CartilageIndexKey[] keys, String inputDirectory, String prefix, PartitionWriter[] writers){

		ReplicatedInputReader r = new ReplicatedInputReader(indexes, keys);
		r.firstPass = false;
		File[] files = new File(inputDirectory).listFiles();

		long startTime = System.nanoTime();
		for (File f : files) {
            if (!f.getName().startsWith(prefix)) {
                continue;
            }
            System.out.println(f.getPath());
			r.scan(f.getPath(), writers);
			for (PartitionWriter writer : writers)
				writer.flush();
		}
		for (PartitionWriter writer : writers)
			writer.flush();
		double time3 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = " + time3 + " sec");
	}

	// Builds replicas with block sampling
	public void build(double samplingRate, int numBuckets, MDIndex[] indexes, CartilageIndexKey[] keys, String inputDirectory, PartitionWriter[] writers){

		long startTime = System.nanoTime();
		for(MDIndex index: indexes)
			index.initBuild(numBuckets);
		ReplicatedInputReader r = new ReplicatedInputReader(indexes, keys);

		File[] files = new File(inputDirectory).listFiles();
		for (File f : files) {
			r.firstPass = true;
			r.scanWithBlockSampling(f.getPath(), samplingRate);
		}
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scanning and sampling time = " + time1 + " sec");

		startTime = System.nanoTime();
		for(MDIndex index: indexes)
			index.initProbe();
		double time2 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Build Time = "+time2+" sec");

		startTime = System.nanoTime();
		for (File f : files) {
			r.scan(f.getPath(), writers);
		}
		for(PartitionWriter writer: writers)
			writer.flush();
		double time3 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Probe Time = " + time3 + " sec");

		startTime = System.nanoTime();
		for (int i = 0; i < indexes.length; i++) {
			PartitionWriter writer = writers[i];
			RobustTreeHs index = (RobustTreeHs)indexes[i];
			byte[] indexBytes = index.marshall();
			writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
			byte[] sampleBytes = index.serializeSample();
			writer.writeToPartition("sample", sampleBytes, 0, sampleBytes.length);
			byte[] infoBytes = keys[i].toString().getBytes();
			writer.writeToPartition("info", infoBytes, 0, infoBytes.length);
			writer.flush();
		}
		double time4 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index+Sample Write Time = "+time4+" sec");

		System.out.println("Total time = " + (time1 + time2 + time3 + time4) + " sec");
	}

	public void build(double samplingRate, int numBuckets, MDIndex index, CartilageIndexKey key, String inputFilename, PartitionWriter writer, int attributes, int replication) {
		int attrPerReplica = attributes / replication;

		Map<Integer, List<Integer>> replicaAttrs = Maps.newHashMap();
		for (int j = 0; j < attributes; j++) {
			int r = j / attrPerReplica >= replication ? replication - 1 : j / attrPerReplica;
			if (!replicaAttrs.containsKey(r))
				replicaAttrs.put(r, new ArrayList<Integer>());
			replicaAttrs.get(r).add(j);
		}

		MDIndex[] indexes = new MDIndex[replication];
		CartilageIndexKey[] keys = new CartilageIndexKey[replication];
		PartitionWriter[] writers = new PartitionWriter[replication];

		for (int i = 0; i < replication; i++) {
			try {
				keys[i] = key.clone();
				keys[i].setKeys(Ints.toArray(replicaAttrs.get(i)));
				indexes[i] = index.clone();
				writers[i] = writer.clone();
				writers[i].setPartitionDir(writer.getPartitionDir() + "/" + i);
				writers[i].createPartitionDir();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}

		build(samplingRate, numBuckets, indexes, keys, inputFilename, writers);
	}

	public void buildReplicatedWithSample(String[] samples, int numBuckets, MDIndex index, CartilageIndexKey key, PartitionWriter writer, int attributes, int replication) {
		int attrPerReplica = attributes / replication;

		Map<Integer, List<Integer>> replicaAttrs = Maps.newHashMap();
		for (int j = 0; j < attributes; j++) {
			int r = j / attrPerReplica >= replication ? replication - 1 : j / attrPerReplica;
			if (!replicaAttrs.containsKey(r))
				replicaAttrs.put(r, new ArrayList<Integer>());
			replicaAttrs.get(r).add(j);
		}

		MDIndex[] indexes = new MDIndex[replication];
		CartilageIndexKey[] keys = new CartilageIndexKey[replication];
		PartitionWriter[] writers = new PartitionWriter[replication];

		for (int i = 0; i < replication; i++) {
			try {
				keys[i] = key.clone();
				keys[i].setKeys(Ints.toArray(replicaAttrs.get(i)));
				indexes[i] = index.clone();
				writers[i] = writer.clone();
				writers[i].setPartitionDir(writer.getPartitionDir() + "/" + i);
				writers[i].createPartitionDir();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}

		for(MDIndex id: indexes)
			id.initBuild(numBuckets);
		ReplicatedInputReader r = new ReplicatedInputReader(indexes, keys);

		long startTime = System.nanoTime();
		for (String sample : samples) {
			r.scanSample(sample);
		}
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Scanning and sampling time = " + time1 + " sec");

		startTime = System.nanoTime();
		for(MDIndex id: indexes)
			id.initProbe();
		double time2 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index Build Time = "+time2+" sec");


		startTime = System.nanoTime();
		for (int i = 0; i < indexes.length; i++) {
			PartitionWriter w = writers[i];
			RobustTreeHs id = (RobustTreeHs)indexes[i];
			byte[] indexBytes = id.marshall();
			w.writeToPartition("index", indexBytes, 0, indexBytes.length);
			byte[] sampleBytes = id.serializeSample();
			w.writeToPartition("sample", sampleBytes, 0, sampleBytes.length);
			byte[] infoBytes = keys[i].toString().getBytes();
			w.writeToPartition("info", infoBytes, 0, infoBytes.length);
			w.flush();
		}
		double time4 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Index+Sample Write Time = "+time4+" sec");

		System.out.println("Total time = " + (time1 + time2 + time4) + " sec");
	}

	public void buildWithSample(CartilageIndexKeySet sample, int numBuckets, MDIndex index, PartitionWriter writer) {
		index.initBuild(numBuckets);
		((RobustTreeHs)index).loadSample(sample);

		long startTime = System.nanoTime();
		index.initProbe();
		System.out.println("BUILD: index building time = " + ((System.nanoTime() - startTime) / 1E9));

		startTime = System.nanoTime();
		byte[] indexBytes = index.marshall();
		writer.writeToPartition("index", indexBytes, 0, indexBytes.length);
		System.out.println("BUILD: index writing time = " + ((System.nanoTime() - startTime) / 1E9));
	}

}
