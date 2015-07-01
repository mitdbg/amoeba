package core.access.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import core.utils.CuratorUtils;
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
import com.google.common.collect.Maps;

import core.access.AccessMethod;
import core.access.AccessMethod.PartitionSplit;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query.FilterQuery;
import core.access.iterator.IteratorRecord;
import core.access.iterator.PartitionIterator;
import core.utils.HDFSUtils;
import core.utils.SchemaUtils.TYPE;

public class SparkJoinInputFormat extends SparkInputFormat {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	
	
	String joinInput1;	// smaller input
	Integer rid1;
	Integer joinKey1;
	
	String joinInput2;	// larger input
	Integer rid2;
	Integer joinKey2;

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		
		queryConf = new SparkQueryConf(job.getConfiguration());
		
		String partitionIdFile = job.getConfiguration().get("PARTITION_ID_FILE");
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
		
		// read the splits for the larger input from a file
		Map<String,List<Integer>> predicateSplit = parsePredicateRanges(partitionIdFile, queryConf.getHadoopHome());
		
		AccessMethod am = new AccessMethod();
		queryConf.setDataset(joinInput1);
		am.init(queryConf);
		
		List<InputSplit> finalSplits = new ArrayList<InputSplit>();
		for(String p: predicateSplit.keySet()){
			
			List<Path> splitFiles = Lists.newArrayList();
			List<Long> lengths = Lists.newArrayList();
			
			// lookup the predicate in the smaller table (filter query)
			long lowVal = Long.parseLong(p.split(",")[0]);
			long highVal = Long.parseLong(p.split(",")[1]);
			Predicate lookupPred1 = new Predicate(joinKey1, TYPE.LONG, lowVal, PREDTYPE.GT);
			Predicate lookupPred2 = new Predicate(joinKey1, TYPE.LONG, highVal, PREDTYPE.LEQ);
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
			for(Integer input1Id: predicateSplit.get(p)){
				for(FileStatus fs: partitionIdFileMap2.get(input1Id)){ // CHECK: changed from Map1 to Map2
					//System.out.println("probe "+fs.getPath());
					splitFiles.add(fs.getPath());
					lengths.add(fs.getLen());
				}
			}
			
			Path[] splitFilesArr = new Path[splitFiles.size()];
			long[] lengthsArr = new long[lengths.size()];
			for(int i=0;i<splitFilesArr.length;i++){
				splitFilesArr[i] = splitFiles.get(i);
				lengthsArr[i] = lengths.get(i);
			}
			SparkFileSplit thissplit = new SparkFileSplit(splitFilesArr, lengthsArr, new PartitionIterator());
			finalSplits.add(thissplit);
		}
		
		LOG.debug("Total # of splits: " + finalSplits.size());
		System.out.println("done with getting splits");

		return finalSplits;
	}
	
	protected Map<String,List<Integer>> parsePredicateRanges(String hdfsFilename, String hadoopHome){
		
		Map<String,List<Integer>> predicatePartitionIds = Maps.newHashMap();
		
		List<String> lines = HDFSUtils.readHDFSLines(hadoopHome, hdfsFilename);
		for(String line: lines){
			String[] tokens = line.split(";");
			List<Integer> partitionIds = Lists.newArrayList();
			for(String idStr: tokens[0].split(","))
				partitionIds.add(Integer.parseInt(idStr));
			predicatePartitionIds.put(tokens[1]+","+tokens[2], partitionIds);
		}
		
		return predicatePartitionIds;
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
