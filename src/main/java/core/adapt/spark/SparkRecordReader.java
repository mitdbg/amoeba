package core.adapt.spark;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import core.adapt.iterator.*;
import core.common.globals.Globals;
import core.common.index.RNode;
import core.common.index.RobustTree;
import core.utils.HDFSUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.adapt.HDFSPartition;
import core.adapt.spark.SparkInputFormat.SparkFileSplit;
import core.utils.CuratorUtils;

public final class SparkRecordReader extends
		RecordReader<LongWritable, IteratorRecord> {

	protected Configuration conf;

	protected SparkFileSplit sparkSplit;
	int numFiles;

	protected PartitionIterator iterator;
	PartitionIteratorMT[] iterators;
	int currentIterator = 0;

	LongWritable key;
	long recordId;
	boolean hasNext;

	CuratorFramework client;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		client = CuratorUtils.createAndStartClient(conf
				.get(SparkQueryConf.ZOOKEEPER_HOSTS));
		sparkSplit = (SparkFileSplit) split;
		iterator = sparkSplit.getIterator();
		key = new LongWritable();
		recordId = 0;
		numFiles = sparkSplit.getNumPaths();
		iterators = new PartitionIteratorMT[numFiles];

		processFiles();
	}

	public class WorkerThread implements Runnable {
		PartitionIteratorMT it;
		public WorkerThread(PartitionIteratorMT it) {
			this.it = it;
		}

		@Override
		public void run() {
			it.compute();
		}
	}

    public void loadTableInfo(String filePath, String table, FileSystem fs) {
        if (Globals.getTableInfo(table) == null) {
			String path = FilenameUtils.getPathNoEndSeparator(filePath);

			if (FilenameUtils.getBaseName(path).contains("partitions")
					|| FilenameUtils.getBaseName(path).contains("repartition")) { // hack
				path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
			}

			if (FilenameUtils.getBaseName(path).contains("data")) { // hack
				path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
			}

			// To remove table name.
			path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));

			// Initialize Globals.
			Globals.loadTableInfo(table, path, fs);
		}
	}

	public RNode getIndexTreeRoot(String filePath, String table, FileSystem fs) {
        String path = FilenameUtils.getPathNoEndSeparator(filePath);

        if (FilenameUtils.getBaseName(path).contains("partitions")
                || FilenameUtils.getBaseName(path).contains("repartition")) { // hack
            path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
        }

        if (FilenameUtils.getBaseName(path).contains("data")) { // hack
            path = FilenameUtils.getPathNoEndSeparator(FilenameUtils.getPath(path));
        }

        // Initialize RobustTree.
        byte[] indexBytes = HDFSUtils.readFile(fs, path + "/index");
        RobustTree tree = new RobustTree(Globals.getTableInfo(table));
        tree.unmarshall(indexBytes);
        return tree.getRoot();
	}

	// Makes a HDFS Partition per class.
	// Creates an iterator per partition.
	// In parallel, process each partition.
	public void processFiles() throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(5);
		RepartitionIteratorMT.BufferManager bm;

		for (int i=0; i<numFiles; i++) {
			Path filePath = sparkSplit.getPath(i);
            final FileSystem fs = filePath.getFileSystem(conf);
            HDFSPartition partition = new HDFSPartition(fs, filePath.toString(),
                        Short.parseShort(conf.get(SparkQueryConf.HDFS_REPLICATION_FACTOR)),
                        client);

            // TODO: Clean this.
			PartitionIteratorMT it;
			if (iterator instanceof PostFilterIterator) {
				it = new PostFilterIteratorMT(partition);
				loadTableInfo(filePath.toString(), ((PostFilterIterator) iterator).getQuery().getTable(), fs);
			} else if (iterator instanceof RepartitionIterator) {
				RNode tree = getIndexTreeRoot(filePath.toString(), ((RepartitionIterator) iterator).getQuery().getTable(), fs);
				it = new RepartitionIteratorMT(partition, bm);
				loadTableInfo(filePath.toString(), ((RepartitionIterator) iterator).getQuery().getTable(), fs);
			} else {
				System.out.println("ERR: Unknown Iterator");
			}
			iterators[i] = it;

			Runnable worker = new WorkerThread(it);
			executor.execute(worker);
		}

		executor.shutdown();
		while (!executor.isTerminated()) {}

		if (bm != null) {
			bm.finish();
		}
	}

//	protected boolean initializeNext() throws IOException {
//		if (currentFile >= sparkSplit.getStartOffsets().length)
//			return false;
//		else {
//			Path filePath = sparkSplit.getPath(currentFile);
//			final FileSystem fs = filePath.getFileSystem(conf);
//			HDFSPartition partition = new HDFSPartition(fs, filePath.toString(),
//					Short.parseShort(conf.get(SparkQueryConf.HDFS_REPLICATION_FACTOR)),
//					client);
//			System.out.println("INFO: Loading path: " + filePath.toString());
//			try {
//				partition.loadNext();
//				iterator.setPartition(partition);
//				currentFile++;
//				return true;
//			} catch (java.lang.OutOfMemoryError e) {
//				System.out.println("ERR: Failed to load " + filePath.toString());
//				System.out.println(e.getMessage());
//				e.printStackTrace();
//				return false;
//			}
//		}
//	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (hasNext) {
			if (iterator.hasNext()) {
				recordId++;
				return true;
			}
			// hasNext = initializeNext();
		}
		/*
		 * do{ if(iterator.hasNext()){ recordId++; return true; } }
		 * while(initializeNext());
		 */

		// System.out.println("Record read = "+recordId);
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		key.set(recordId);
		return key;
	}

	@Override
	public IteratorRecord getCurrentValue() throws IOException,
			InterruptedException {
		IteratorRecord record = iterator.next();
		return record;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) 0 / sparkSplit.getStartOffsets().length;
	}

	@Override
	public void close() throws IOException {
		iterator.finish(); // this method could even be called earlier in case
							// the entire split does not fit in main-memory
	}
}
