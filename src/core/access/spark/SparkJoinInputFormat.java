package core.access.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import core.access.PartitionRange;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query;
import core.access.Query.FilterQuery;
import core.adapt.opt.Optimizer;
import core.index.MDIndex;
import core.index.robusttree.RobustTreeHs;
import core.utils.Range;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

import core.access.AccessMethod;
import core.access.AccessMethod.PartitionSplit;
import core.access.iterator.IteratorRecord;
import core.access.iterator.PartitionIterator;
import core.utils.HDFSUtils;
import core.utils.SchemaUtils.TYPE;
import scala.Tuple2;

public class SparkJoinInputFormat extends SparkInputFormat {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	private static final long MAX_SPLIT_SIZE = 160000 * 1000000L;
	private static final int SPLIT_FANOUT = 4;
	private static final double OVERLAP_THRESHOLD = 0.75;
	
	String joinInput1;	// smaller input
	Integer rid1;
	Integer joinKey1;
	
	String joinInput2;	// larger input
	Integer rid2;
	Integer joinKey2;

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		
		queryConf = new SparkQueryConf(job.getConfiguration());
		
		boolean assignBuckets = job.getConfiguration().getBoolean("ASSIGN_BUCKETS", true);
		joinInput1 = job.getConfiguration().get("JOIN_INPUT1");
		joinInput2 = job.getConfiguration().get("JOIN_INPUT2");
		String joinCond = job.getConfiguration().get("JOIN_CONDITION");
		String tokens[] = joinCond.split("=");
		rid1 = Integer.parseInt(tokens[0].split("\\.")[0]);
		joinKey1 = Integer.parseInt(tokens[0].split("\\.")[1]);
		rid2 = Integer.parseInt(tokens[1].split("\\.")[0]);
		joinKey2 = Integer.parseInt(tokens[1].split("\\.")[1]);

		// hack
		System.out.println("reading from input: "+joinInput1);
		job.getConfiguration().set(FileInputFormat.INPUT_DIR, "hdfs://istc2.csail.mit.edu:9000"+joinInput1);	// read hadoop namenode from conf		
		List<FileStatus> files = listStatus(job);
		System.out.println("files from first join input: "+files.size());
		
		System.out.println("reading from input: "+joinInput2);
		job.getConfiguration().set(FileInputFormat.INPUT_DIR, "hdfs://istc2.csail.mit.edu:9000"+joinInput2);
		files.addAll(listStatus(job));
		System.out.println("files from both join inputs: "+files.size());
		
		
		ArrayListMultimap<Integer,FileStatus> partitionIdFileMap1 = ArrayListMultimap.create(); // partitions in smaller table
		ArrayListMultimap<Integer,FileStatus> partitionIdFileMap2 = ArrayListMultimap.create(); // partitions in lineitem
		for(FileStatus file: files){
			try {
				String hdfsPath = file.getPath().toString();
				int id = Integer.parseInt(FilenameUtils.getName(hdfsPath));
				if(hdfsPath.contains(joinInput1))
					partitionIdFileMap1.put(id, file);
				else if(hdfsPath.contains(joinInput2))
					partitionIdFileMap2.put(id, file);
			} catch (NumberFormatException e) {
			}
		}
		
		AccessMethod am = new AccessMethod();
		queryConf.setDataset(joinInput1);
		am.init(queryConf);

		// get bucket splits from larger table
		Optimizer opt = new Optimizer(joinInput2, queryConf.getHadoopHome());
		opt.loadIndex(queryConf.getZookeeperHosts());

		RobustTreeHs index = opt.getIndex();
		Map<Integer, MDIndex.BucketInfo> ranges = index.getBucketRanges(joinKey2);

		Object[] cutpoints = index.sample.getCutpoints(joinKey2, SPLIT_FANOUT);
		TYPE type = index.sample.getTypes()[joinKey2];

		List<Tuple2<Range, List<Integer>>> bigSplits;
		if (assignBuckets) { // assign bucket ids from larger tables to ranges
			List<PartitionRange> partitions = new ArrayList<PartitionRange>();
			int rangeSize = 1;
			int currentStart = 0;
			Range fullRange = new Range(type, cutpoints[0], cutpoints[cutpoints.length-1]);
			fullRange.expand(0.001);
			cutpoints[0] = fullRange.getLow();
			cutpoints[cutpoints.length-1] = fullRange.getHigh();
			while (rangeSize <= SPLIT_FANOUT) {
				while (currentStart < (cutpoints.length - rangeSize)) {
					partitions.add(new PartitionRange(type, cutpoints[currentStart], cutpoints[currentStart + rangeSize]));
					currentStart += rangeSize;
				}
				rangeSize *= 2;
				currentStart = 0;
			}
			System.out.println(partitions);

			bigSplits = assignBucketsToSplits(partitions, fullRange, ranges, partitionIdFileMap2);
		} else { // use index on larger table to get bucket ids for each range
			bigSplits = new ArrayList<Tuple2<Range, List<Integer>>>();
			List<PartitionRange> partitions = new ArrayList<PartitionRange>();
			int rangeSize = 1;
			int currentStart = 0;
			Range fullRange = new Range(type, cutpoints[0], cutpoints[cutpoints.length-1]);
			fullRange.expand(0.001);
			cutpoints[0] = fullRange.getLow();
			cutpoints[cutpoints.length-1] = fullRange.getHigh();
			while (currentStart < cutpoints.length) {
				partitions.add(new PartitionRange(type, cutpoints[currentStart], cutpoints[currentStart + rangeSize]));
				currentStart += rangeSize;
			}

			for (PartitionRange r : partitions) {
				Predicate lookupPred1 = new Predicate(joinKey2, r.getType(), r.getLow(), PREDTYPE.GT);
				Predicate lookupPred2 = new Predicate(joinKey2, r.getType(), r.getHigh(), PREDTYPE.LEQ);

				List<Integer> ids = new ArrayList<Integer>();
				PartitionSplit[] optSplits = opt.buildAccessPlan(new Query.FilterQuery(new Predicate[]{lookupPred1, lookupPred2}));
				for (PartitionSplit s : optSplits) {
					for (int i : s.getPartitions()) {
						ids.add(i);
					}
				}
				bigSplits.add(new Tuple2(r, ids));
			}
		}

		List<InputSplit> finalSplits = new ArrayList<InputSplit>();
		for(Tuple2<Range, List<Integer>> p: bigSplits){
			
			List<Path> splitFiles = Lists.newArrayList();
			List<Long> lengths = Lists.newArrayList();
			
			// lookup the predicate in the smaller table (filter query)
			// int vs long distinction... won't work for other types of keys
			Predicate lookupPred1 = new Predicate(joinKey1, TYPE.LONG, ((Integer)p._1().getLow()).longValue(), PREDTYPE.GT);
			Predicate lookupPred2 = new Predicate(joinKey1, TYPE.LONG, ((Integer)p._1().getHigh()).longValue(), PREDTYPE.LEQ);
			System.out.println("predicate1: "+lookupPred1);
			System.out.println("predicate2: "+lookupPred2);

			// ids from smaller table that match this range of values
			PartitionSplit[] splits = am.getPartitionSplits(new FilterQuery(new Predicate[]{lookupPred1,lookupPred2}), 100, true);
			
			// add files from the smaller input first (build input)
			for(PartitionSplit split: splits){
				int[] partitionIds = split.getPartitions();
				for(int i=0;i<partitionIds.length;i++){
					for(FileStatus fs: partitionIdFileMap1.get(partitionIds[i])){ // CHECK: from Map2 to Map1
						//System.out.println(fs.getPath());
						splitFiles.add(fs.getPath());
						lengths.add(fs.getLen());
					}
					System.out.print(partitionIds[i]+",");
				}
			}
			System.out.println();
			System.out.println("number of files from the smaller input: "+ splitFiles.size());


			// add files from the larger input (probe input)
			for(Integer input1Id: p._2()){
				for(FileStatus fs: partitionIdFileMap2.get(input1Id)){ // CHECK: changed from Map1 to Map2
					//System.out.println("probe "+fs.getPath());
					splitFiles.add(fs.getPath());
					lengths.add(fs.getLen());
				}
				System.out.print(input1Id+",");
			}
			System.out.println();

			long splitSize = 0;
			Path[] splitFilesArr = new Path[splitFiles.size()];
			long[] lengthsArr = new long[lengths.size()];
			for(int i=0;i<splitFilesArr.length;i++){
				splitFilesArr[i] = splitFiles.get(i);
				lengthsArr[i] = lengths.get(i);
				splitSize += lengthsArr[i];
			}
			System.out.println("Size of split: "+splitSize);
			SparkFileSplit thissplit = new SparkFileSplit(splitFilesArr, lengthsArr, new PartitionIterator());
			finalSplits.add(thissplit);
		}
		
		System.out.println("Total # of splits: " + finalSplits.size());
		System.out.println("done with getting splits");

		return finalSplits;
	}

	protected List<Tuple2<String, List<Integer>>> parsePredicateRanges(String hdfsFilename, String hadoopHome){
		List<Tuple2<String, List<Integer>>> predicatePartitionIds = new ArrayList<Tuple2<String, List<Integer>>>();

		List<String> lines = HDFSUtils.readHDFSLines(hadoopHome, hdfsFilename);
		for(String line: lines){
			String[] tokens = line.split(";");
			List<Integer> partitionIds = Lists.newArrayList();
			for(String idStr: tokens[0].split(","))
				partitionIds.add(Integer.parseInt(idStr));
			predicatePartitionIds.add(new Tuple2(tokens[1]+","+tokens[2], partitionIds));
		}

		return predicatePartitionIds;
	}

	private static List<Tuple2<Range, List<Integer>>> assignBucketsToSplits(List<PartitionRange> ranges, Range fullRange, Map<Integer, MDIndex.BucketInfo> bucketRanges, ArrayListMultimap<Integer,FileStatus> partitionFileStatuses) {
		List<MDIndex.BucketInfo> unassignedBuckets = new ArrayList<MDIndex.BucketInfo>();

		// For every bucket, try to assign it to one of the pre-calculated ranges
		for (Map.Entry<Integer, MDIndex.BucketInfo> bucketRange : bucketRanges.entrySet()) {
			if (!partitionFileStatuses.containsKey(bucketRange.getKey())) {
				continue;
			}

			MDIndex.BucketInfo info = bucketRange.getValue();
			boolean inserted = false;
			for (PartitionRange r : ranges) {
				info.intersect(fullRange);
				if (r.contains(info)) {
					r.addBucket(info);
					inserted = true;
					break;
				} else {
					if (info.intersectionFraction(r) > OVERLAP_THRESHOLD) {
						r.addBucket(info);
						inserted = true;
						break;
					}
				}
			}
			if (!inserted) {
				unassignedBuckets.add(info);
			}
		}
		
		for (MDIndex.BucketInfo info : unassignedBuckets) {
			for (PartitionRange r : ranges) {
				if (info.intersectionFraction(r) > 0) {
					r.addBucket(info, false);
				}
			}
		}

		System.out.println("Ranges formed: "+ranges);
		System.out.println("Unassigned buckets: "+unassignedBuckets);

		// Split up any ranges of buckets that are too large
		List<Tuple2<Range, List<Integer>>> splits = new ArrayList<Tuple2<Range, List<Integer>>>();
		for (PartitionRange r : ranges) {
			List<MDIndex.BucketInfo> buckets = r.getBuckets();
			System.out.println(buckets.size() + " buckets in "+r);

			long bucketSize = 0;
			for (MDIndex.BucketInfo bucket : buckets) {
				List<FileStatus> files = partitionFileStatuses.get(bucket.getId());
				for (FileStatus f : files) {
					bucketSize += f.getLen();
				}
			}
			int numSplitsRequired = (int) (bucketSize / MAX_SPLIT_SIZE + 1);
			long goalSize = bucketSize / numSplitsRequired + 1;

			int currentIndex = 0;
			List<Integer> currentBuckets = new ArrayList<Integer>();
			long totalSize = 0;
			while (currentIndex < buckets.size()) {
				while (totalSize < goalSize && currentIndex < buckets.size()) {
					MDIndex.BucketInfo bucket = buckets.get(currentIndex);
					List<FileStatus> files = partitionFileStatuses.get(bucket.getId());
					currentBuckets.add(bucket.getId());
					for (FileStatus f : files) {
						totalSize += f.getLen();
					}
					currentIndex++;
				}
				splits.add(new Tuple2(r, currentBuckets));

				currentBuckets = new ArrayList<Integer>();
				totalSize = 0;
			}
		}
		return splits;
	}

	@Override
	public RecordReader<LongWritable, IteratorRecord> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new SparkRecordReader(){
			long relationId;

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

				System.out.println("Initializing SparkRecordReader");

				conf = context.getConfiguration();
				//client = CuratorUtils.createAndStartClient(conf.get(SparkQueryConf.ZOOKEEPER_HOSTS));
				client = null;
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
				boolean flag = super.initializeNext();
				if(flag) {
					relationId = sparkSplit.getPath(currentFile-1).toString().contains("repl") ? 1 : 0; // CHECK
					//relationId = sparkSplit.getPath(currentFile-1).toString().contains(joinInput1) ? rid1 : rid2;
					//System.out.println(sparkSplit.getPath(currentFile-1).toString()+" "+relationId);
				}
				return flag;
			}
			public LongWritable getCurrentKey() throws IOException, InterruptedException {
				key.set(relationId);
				return key;
			}
		};
	}
}
