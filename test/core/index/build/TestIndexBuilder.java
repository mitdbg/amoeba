package core.index.build;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import core.index.MDIndex;
import core.index.key.CartilageIndexKeySet;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;
import core.utils.SchemaUtils;
import junit.framework.TestCase;
import core.index.Settings;
import core.index.SimpleRangeTree;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeyMT;
import core.index.robusttree.RobustTreeHs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class TestIndexBuilder extends TestCase {

	String inputFilename;
	CartilageIndexKey key;
	IndexBuilder builder;

	int numPartitions;
	int partitionBufferSize;

	String localPartitionDir;
	String hdfsPartitionDir;

	String propertiesFile;
	int attributes;
	int replication;

	@Override
	public void setUp(){
		inputFilename = Settings.tpchPath + "lineitem.tbl";
		partitionBufferSize = 5*1024*1024;
		numPartitions = 8200;

		localPartitionDir = Settings.localPartitionDir;
		hdfsPartitionDir = Settings.hdfsPartitionDir;
		propertiesFile = Settings.cartilageConf;

		key = new CartilageIndexKey('|');
		//key = new SinglePassIndexKey('|');
		builder = new IndexBuilder();

		attributes = Settings.numAttributes;
		replication = 3;
	}

	private PartitionWriter getLocalWriter(String partitionDir){
		return new BufferedPartitionWriter(partitionDir, partitionBufferSize, numPartitions);
	}

	private PartitionWriter getHDFSWriter(String partitionDir, short replication){
		return new HDFSPartitionWriter(partitionDir, partitionBufferSize, numPartitions, replication, propertiesFile);
	}

	public void testReader(){
		long startTime = System.nanoTime();
		InputReader r = new InputReader(new SimpleRangeTree(numPartitions), key);
		r.scan(inputFilename);
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Time = "+time1+" sec");
	}

	public void testReaderMultiThreaded(){
		long startTime = System.nanoTime();
		int numThreads = 1;
		CartilageIndexKey[] keys = new CartilageIndexKey[numThreads];
		for(int i=0; i<keys.length; i++)
			keys[i] = new CartilageIndexKeyMT('|');

		InputReaderMT r = new InputReaderMT(new SimpleRangeTree(numPartitions), keys);
		r.scan(inputFilename, numThreads);
		double time1 = (System.nanoTime()-startTime)/1E9;
		System.out.println("Time = " + time1 + " sec");
	}

	public void testBuildSimpleRangeTreeLocal(){
		builder.build(new SimpleRangeTree(numPartitions),
				key,
				inputFilename,
				getLocalWriter(localPartitionDir)
		);
	}

    public void testBuildKDMedianTreeLocal(){
        File f = new File(inputFilename);
        Runtime runtime = Runtime.getRuntime();
        double samplingRate = runtime.freeMemory() / (2.0 * f.length());
        System.out.println("Sampling rate: "+samplingRate);
        builder.build(
				new KDMedianTree(samplingRate),
				key,
				inputFilename,
				getLocalWriter(localPartitionDir)
		);
    }

	public void testBuildKDMedianTreeBlockSamplingOnly(int scaleFactor) {
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * 759) / bucketSize + 1;
		System.out.println("Num buckets: "+numBuckets);
		builder.buildWithBlockSamplingDir(0.0002,
				numBuckets,
				new KDMedianTree(1),
				key,
				Settings.tpchPath + scaleFactor + "/");
	}

	public void testBuildSimpleRangeTreeLocalReplicated(){
		File f = new File(inputFilename);
		long fileSize = f.length();
		int bucketSize = 64 * 1024 * 1024;
		int numBuckets = (int) (fileSize / bucketSize) + 1;
		builder.build(1,
				numBuckets,
				new SimpleRangeTree(numPartitions),
				key,
				Settings.tpchPath,
				getLocalWriter(localPartitionDir),
				attributes,
				replication
		);
	}

	public void testBuildSimpleRangeTreeHDFS(){
		builder.build(new SimpleRangeTree(numPartitions),
						key,
						inputFilename,
						getHDFSWriter(hdfsPartitionDir, (short) replication)
					);
	}

	public void testBuildSimpleRangeTreeHDFSReplicated(){
		File f = new File(inputFilename);
		long fileSize = f.length();
		int bucketSize = 64 * 1024 * 1024;
		int numBuckets = (int) (fileSize / bucketSize) + 1;
		builder.build(1,
				numBuckets,
				new SimpleRangeTree(numPartitions),
				key,
				Settings.tpchPath,
				getHDFSWriter(hdfsPartitionDir, (short)replication),
				attributes,
				replication
			);
	}

	public void testBuildRobustTree(){
		builder.build(new RobustTreeHs(0.01),
				key,
				inputFilename,
				getHDFSWriter(hdfsPartitionDir, (short) replication));
	}

	public void testBuildRobustTreeBlockSampling() {
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);
		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		client.close();
		builder.buildWithBlockSampling(0.0002,
				new RobustTreeHs(1),
				key,
				inputFilename,
				getHDFSWriter(hdfsPartitionDir, (short) replication));
	}

	public void testBuildRobustTreeBlockSamplingOnly(int scaleFactor) {
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * 759) / bucketSize + 1;
		builder.buildWithBlockSamplingDir(0.0002,
				numBuckets,
				new RobustTreeHs(1),
				key,
				Settings.tpchPath + scaleFactor + "/");
	}

	public void testWritePartitionsFromIndex(String partitionsId) {
		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/index");
		RobustTreeHs index = new RobustTreeHs(1);
		index.unmarshall(indexBytes);
		builder.buildDistributedFromIndex(index,
				key,
				Settings.tpchPath,
				getHDFSWriter(hdfsPartitionDir + "/partitions" + partitionsId, (short) replication));

	}

	public void testWritePartitionsFromIndexReplicated(String partitionsId){
		ConfUtils conf = new ConfUtils(propertiesFile);
		FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
		MDIndex[] indexes = new MDIndex[replication];
		CartilageIndexKey[] keys = new CartilageIndexKey[replication];
		PartitionWriter[] writers = new PartitionWriter[replication];
		long start = System.nanoTime();
		for (int i = 0; i < replication; i++) {
			byte[] indexBytes = HDFSUtils.readFile(fs, hdfsPartitionDir + "/" + i + "/index");
			RobustTreeHs index = new RobustTreeHs(1);
			index.unmarshall(indexBytes);
			indexes[i] = index;

			String keyString = new String(HDFSUtils.readFile(fs, hdfsPartitionDir + "/" + i + "/info"));
			keys[i] = new CartilageIndexKey(keyString);

			writers[i] = getHDFSWriter(hdfsPartitionDir + "/" + i + "/partitions"+ partitionsId, (short)1);
		}
		builder.buildDistributedReplicasFromIndex(indexes,
				keys,
				Settings.tpchPath,
				"orders.tbl.",
				writers);
		System.out.println("PARTITIONING TOTAL for replica: " + (System.nanoTime() - start) / 1E9);
	}

	public void testBuildReplicatedRobustTree(int scaleFactor, int numReplicas){
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * 759) / bucketSize + 1;
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);
		CuratorFramework client = CuratorUtils.createAndStartClient(cfg.getZOOKEEPER_HOSTS());
		CuratorUtils.deleteAll(client, "/", "partition-");
		client.close();
		builder.build(0.0002,
				numBuckets,
				new RobustTreeHs(1),
				key,
				Settings.tpchPath,
				getHDFSWriter(hdfsPartitionDir, (short) replication),
				attributes,
				numReplicas
		);
	}

	public void testBuildReplicatedRobustTreeFromSamples(int scaleFactor, int numReplicas) {
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		List<String> paths = new ArrayList<String>();
		try {
			RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(Settings.hdfsPartitionDir), false);
			while (itr.hasNext()) {
				paths.add(itr.next().getPath().getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String[] samples = new String[paths.size()];
		for (int i = 0; i < samples.length; i++) {
			samples[i] =  new String(HDFSUtils.readFile(fs, Settings.hdfsPartitionDir+"/"+paths.get(i)));
		}
		//String sample = new String(HDFSUtils.readFile(fs, Settings.hdfsPartitionDir+"/sample"));
		int bucketSize = 64; // 64 mb
		int numBuckets = (scaleFactor * Settings.tpchSize) / bucketSize + 1;
		builder.buildReplicatedWithSample(
				samples,
				numBuckets,
				new RobustTreeHs(1),
				key,
				getHDFSWriter(hdfsPartitionDir, (short) replication),
				attributes,
				numReplicas
		);
	}

	public static void main(String[] args){
		System.out.println("IMBA!");
		TestIndexBuilder t = new TestIndexBuilder();
		t.setUp();
		t.testWritePartitionsFromIndexReplicated(args[args.length - 1]);
		//int scaleFactor = Integer.parseInt(args[args.length - 1]);
		//t.testBuildKDMedianTreeBlockSamplingOnly(scaleFactor);
		//t.testBuildRobustTreeBlockSampling();
		//t.testBuildReplicatedRobustTreeFromSamples(scaleFactor, 3);
	}
}
