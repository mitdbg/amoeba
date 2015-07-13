package core.opt.simulator;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;

import core.index.MDIndex;
import core.index.MDIndex.Bucket;
import core.index.Settings;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

/**
 * Used to create index of varying sizes
 * @author anil
 *
 */
public class IndexBuilder extends TestCase {
	public void testBuildIndices() {
		String hdfsPath = "hdfs://localhost:9000/user/anil/test";
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);

		int[] rounds = new int[]{1,2,5,10,20,50,100};
		for (int i=0; i<rounds.length; i++) {
			long sf = rounds[i]; // Scale-Factor
			System.out.println("Building index with sf:" + sf);

			long fileSize = sf * 759863287;
			long bucketSize = 64*1024*1024;
			int maxBuckets = (int) (fileSize / bucketSize) + 1;

			FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
			RobustTreeHs index = new RobustTreeHs(0.01);
			MDIndex.Bucket.counters = new MDIndex.BucketCounts(cfg.getZOOKEEPER_HOSTS());

			
			String pathToSample = hdfsPath + "/sample";
			byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
	        index.loadSampleAndBuild(maxBuckets, sampleBytes);

			index.initProbe();
			byte[] indexBytes = index.marshall();
			HDFSUtils.writeFile(fs, hdfsPath + "/index_" + sf, (short)1, indexBytes, 0, indexBytes.length, false);
		}
	}

	public void testSmallIndex() {
		String hdfsPath = "hdfs://localhost:9000/user/anil/test";
		ConfUtils cfg = new ConfUtils(Settings.cartilageConf);

		int maxBuckets = 1024;
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		RobustTreeHs index = new RobustTreeHs(0.01);
		Bucket.counters = new MDIndex.BucketCounts(cfg.getZOOKEEPER_HOSTS());
		
		String pathToSample = hdfsPath + "/sample";
		byte[] sampleBytes = HDFSUtils.readFile(fs, pathToSample);
        index.loadSampleAndBuild(maxBuckets, sampleBytes);

		index.initProbe();
		byte[] indexBytes = index.marshall();
		HDFSUtils.writeFile(fs, hdfsPath + "/index_", (short)1, indexBytes, 0, indexBytes.length, false);
	}
}