package core.access.spark;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.access.HDFSPartition;
import core.access.iterator.IteratorRecord;
import core.access.iterator.PartitionIterator;
import core.access.spark.SparkInputFormat.SparkFileSplit;
import core.utils.CuratorUtils;

public class SparkRecordReader extends RecordReader<LongWritable, IteratorRecord> {

	protected Configuration conf;
	
	protected SparkFileSplit sparkSplit;
	int currentFile;
	
	protected PartitionIterator iterator;

	LongWritable key;
	long recordId;
	boolean hasNext;

	CuratorFramework client;
	//BucketCounts counter;
	//PartitionLock locker;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		System.out.println("Initializing SparkRecordReader");
		
		conf = context.getConfiguration();
		client = CuratorUtils.createAndStartClient(conf.get(SparkQueryConf.ZOOKEEPER_HOSTS));		
		sparkSplit = (SparkFileSplit)split;
		
		iterator = sparkSplit.getIterator();
		currentFile = 0;
		
		//FileSystem fs = sparkSplit.getPath(currentFile).getFileSystem(conf);
		//counter = new BucketCounts(fs, conf.get(SparkQueryConf.COUNTERS_FILE));
		//locker = new PartitionLock(fs, conf.get(SparkQueryConf.LOCK_DIR));		
		
		hasNext = initializeNext();		
		key = new LongWritable();
		recordId = 0;		
	}

	protected boolean initializeNext() throws IOException{
		
		if(currentFile>0)
			System.out.println("Records read = "+recordId);
		
		if(currentFile >= sparkSplit.getStartOffsets().length)
			return false;
		else{
			Path filePath = sparkSplit.getPath(currentFile);
			final FileSystem fs = filePath.getFileSystem(conf);
			HDFSPartition partition = new HDFSPartition(fs, filePath.toString(), client);
			System.out.println("loading path: "+filePath.toString());
			try {
				partition.loadNext();
				iterator.setPartition(partition);
				currentFile++;
				return true;
			} catch (java.lang.OutOfMemoryError e) {
				System.out.println("ERR: Failed to load " + filePath.toString());
				System.out.println(e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (hasNext) {
			if(iterator.hasNext()){
				recordId++;
				return true;
			}
			hasNext = initializeNext();
		}
		/*
		do{
			if(iterator.hasNext()){
				recordId++;
				return true;
			}	
		} while(initializeNext());*/
		
		//System.out.println("Record read = "+recordId);
		return false;		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		key.set(recordId);
		return key;
	}

	@Override
	public IteratorRecord getCurrentValue() throws IOException, InterruptedException {
		return iterator.next();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)currentFile / sparkSplit.getStartOffsets().length;
	}

	@Override
	public void close() throws IOException {
		iterator.finish();		// this method could even be called earlier in case the entire split does not fit in main-memory
		//counter.close();
		//locker.cleanup();
	}
}
