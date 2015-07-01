package core.access.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;

import core.access.AccessMethod;
import core.access.AccessMethod.PartitionSplit;
import core.access.Query.FilterQuery;
import core.access.iterator.DistributedRepartitionIterator;
import core.access.iterator.IteratorRecord;
import core.access.iterator.PartitionIterator;
import core.access.iterator.PostFilterIterator;
import core.access.iterator.RepartitionIterator;
import core.utils.ReflectionUtils;

public class SparkInputFormat extends FileInputFormat<LongWritable, IteratorRecord> implements Serializable {

	public static String SPLIT_ITERATOR = "SPLIT_ITERATOR";
	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	SparkQueryConf queryConf;


	public static class SparkFileSplit extends CombineFileSplit implements Serializable {
		private static final long serialVersionUID = 1L;

		private PartitionIterator iterator;
		public SparkFileSplit(){
		}

		public SparkFileSplit(Path[] files, long[] lengths, PartitionIterator iterator) {
			super(files, lengths);
			this.iterator = iterator;
		}

		public SparkFileSplit(Path[] files, long[] start, long[] lengths, String[] locations, PartitionIterator iterator) {
			super(files, start, lengths, locations);
			this.iterator = iterator;
		}
		
		public PartitionIterator getIterator(){
			return this.iterator;
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			super.write(out);
			Text.writeString(out, iterator.getClass().getName());
			iterator.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException{
			super.readFields(in);
			iterator = (PartitionIterator)ReflectionUtils.getInstance(Text.readString(in));
			iterator.readFields(in);
		}

		public static SparkFileSplit read(DataInput in) throws IOException {
			SparkFileSplit s = new SparkFileSplit();
	        s.readFields(in);
	        return s;
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		List<InputSplit> finalSplits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);

		queryConf = new SparkQueryConf(job.getConfiguration());
		AccessMethod am = new AccessMethod();
		am.init(queryConf);
		// am.init(queryConf.getDataset(), queryConf.getHadoopHome());

		ArrayListMultimap<Integer,FileStatus> partitionIdFileMap = ArrayListMultimap.create();
		Map<Integer,Long> partitionIdSizeMap = Maps.newHashMap();
		for(FileStatus file: files){
			try {
				int id = Integer.parseInt(FilenameUtils.getName(file.getPath().toString()));
				partitionIdFileMap.put(id, file);
				long currentSize = partitionIdSizeMap.containsKey(id) ? partitionIdSizeMap.get(id) : 0;
				partitionIdSizeMap.put(id, currentSize + file.getLen());
			} catch (NumberFormatException e) {
			}
		}

		PartitionSplit[] splits;
		if(queryConf.getFullScan()){
				splits = new PartitionSplit[]{ new PartitionSplit(
													Ints.toArray(partitionIdFileMap.keySet()),
													new PostFilterIterator(new FilterQuery(queryConf.getPredicates(), am.getKey()))
													)
											};
		}
		else if (queryConf.getRepartitionScan()) {
			splits = new PartitionSplit[]{new PartitionSplit(
					Ints.toArray(partitionIdFileMap.keySet()),
					new DistributedRepartitionIterator(new FilterQuery(queryConf.getPredicates(), am.getKey()), am.getIndex().getRoot())
			)};
		}
		else
			splits = am.getPartitionSplits(new FilterQuery(queryConf.getPredicates(), am.getKey()), queryConf.getWorkers(), queryConf.getJustAccess());
		System.out.println("Number of partition splits = "+splits.length);
		//splits = resizeSplits(splits, partitionIdFileMap, queryConf.getMaxSplitSize());
		splits = resizeSplits(splits, partitionIdSizeMap, queryConf.getMaxSplitSize(), queryConf.getMinSplitSize());
		System.out.println("Number of partition splits after splitting= "+splits.length);
		//splits = combineSplits(splits, queryConf.getMinSplitSize(), queryConf.getMaxSplitSize());
		//System.out.println("Number of partition splits after combining= "+splits.length);
		for (PartitionSplit split: splits) {
			System.out.println("SPLIT: " + split.getIterator().getClass().getName() + " buckets: " + Arrays.toString(split.getPartitions()));
		}

		for(PartitionSplit split: splits){
			int[] partitionIds = split.getPartitions();
			List<Path> splitFiles = Lists.newArrayList();
			List<Long> lengths = Lists.newArrayList();

			for(int i=0;i<partitionIds.length;i++){
//				System.out.println(partitionIds[i]);
				for(FileStatus fs: partitionIdFileMap.get(partitionIds[i])){
					splitFiles.add(fs.getPath());
					lengths.add(fs.getLen());
				}
			}

			PartitionIterator itr = split.getIterator();
			if(itr instanceof RepartitionIterator || itr instanceof DistributedRepartitionIterator)		// hack to set the zookeeper hosts
				((RepartitionIterator)itr).setZookeeper(queryConf.getZookeeperHosts());

			Path[] splitFilesArr = new Path[splitFiles.size()];
			long[] lengthsArr = new long[lengths.size()];
			for(int i=0;i<splitFilesArr.length;i++){
				splitFilesArr[i] = splitFiles.get(i);
				lengthsArr[i] = lengths.get(i);
			}
			
			SparkFileSplit thissplit = new SparkFileSplit(splitFilesArr, lengthsArr, itr);
			finalSplits.add(thissplit);
		}

		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		LOG.debug("Total # of splits: " + finalSplits.size());
		System.out.println("done with getting splits");

		return finalSplits;
	}
	
//	private Map<Integer,Long> getPartitionIdMap(PartitionSplit[] splits, Multimap<Integer,FileStatus> partitionIdFileMap){
//		Map<Integer,Long> partitionIdSizeMap = Maps.newHashMap();
//		
//		for(PartitionSplit split: splits){
//			int[] partitionIds = split.getPartitions();
//			
//			for(int pid: partitionIds){
//				long size = 0;
//				for(FileStatus fs: partitionIdFileMap.get(pid))
//					size += fs.getLen();
//				size = size / 1024 / 1024;
//				//System.out.println("partition="+pid+", size="+size);
//				if(partitionIdSizeMap.containsKey(pid))
//					partitionIdSizeMap.put(pid, partitionIdSizeMap.get(pid) + size);
//				else
//					partitionIdSizeMap.put(pid, size);
//			}
//			
//		}		
//		return partitionIdSizeMap;
//	}
	
	private long getPartitionSplitSize(PartitionSplit split, Map<Integer,Long> partitionIdSizeMap){
		if(partitionIdSizeMap==null){
			System.err.println("partition size map is null");
			System.exit(0);
		}
		long size = 0;
		for(int pid: split.getPartitions()){
			if(partitionIdSizeMap.containsKey(pid))
				size += partitionIdSizeMap.get(pid);
			else
				System.err.println("partitoion size not found: "+pid);
		}
		return size;
	}



	/**
	 * The goal of this method is to check the size of each and break large splits into
	 * multiple smaller splits.
	 *
	 * The maximum split size is read from the configuration and it depends on the size of each machine.
	 *
	 * @param initialSplits
	 * @return
	 */
	public PartitionSplit[] resizeSplits(PartitionSplit[] initialSplits, Map<Integer,Long> partitionSizes, long maxSplitSize, long minSplitSize){
		
		List<PartitionSplit> resizedSplits = Lists.newArrayList();
		ArrayListMultimap<String,PartitionSplit> smallSplits = ArrayListMultimap.create();
		
		for(PartitionSplit split: initialSplits){
			long splitSize = getPartitionSplitSize(split, partitionSizes);
			if(splitSize > maxSplitSize){
				// create smaller splits
				resizedSplits.addAll(createSmaller(split, partitionSizes, maxSplitSize));
			}
			else if(splitSize < minSplitSize){
				// create larger splits
				smallSplits.put(split.getIterator().getClass().getName(), split);
			}
			else{
				// just accept as it is
				PartitionIterator itr = split.getIterator();
				if(itr instanceof RepartitionIterator)
					itr = ((RepartitionIterator)itr).createDistributedIterator();
				resizedSplits.add(new PartitionSplit(split.getPartitions(), itr));
			}
		}
		
		for(String key: smallSplits.keySet())
			resizedSplits.addAll(createLarger(smallSplits.get(key), partitionSizes, maxSplitSize));
		
		return resizedSplits.toArray(new PartitionSplit[resizedSplits.size()]);
	}
	
	private List<PartitionSplit> createLarger(List<PartitionSplit> splits, Map<Integer,Long> partitionSizes, long maxSplitSize){
		List<PartitionSplit> largerSplits = Lists.newArrayList();
		
		Multimap<Integer, Integer> largerSplitPartitionIds = ArrayListMultimap.create();
		long currentSize = 0; int largerSplitId = 0;
		
		for(PartitionSplit split: splits){
			for(Integer p: split.getPartitions()){
				long pSize = partitionSizes.containsKey(p) ? partitionSizes.get(p) : 0;
				if(currentSize + pSize > maxSplitSize){
					largerSplitId++;
					currentSize = 0;
				}
				currentSize += pSize;
				largerSplitPartitionIds.put(largerSplitId, p);
			}
		}
		
		for(Integer k: largerSplitPartitionIds.keySet()){
			PartitionIterator itr = splits.get(0).getIterator();
			if(itr instanceof RepartitionIterator)
				itr = ((RepartitionIterator)itr).createDistributedIterator();
			largerSplits.add(new PartitionSplit(Ints.toArray(largerSplitPartitionIds.get(k)), itr));
		}
		
		return largerSplits;
	}
	
	private List<PartitionSplit> createSmaller(PartitionSplit split, Map<Integer,Long> partitionSizes, long maxSplitSize){
		List<PartitionSplit> smallerSplits = Lists.newArrayList();
		
		int[] partitions = split.getPartitions();
		long currentSize = 0;
		Multimap<Integer, Integer> splitPartitionIds = ArrayListMultimap.create();
		int splitId = 0;
		
		for(int i=0; i<partitions.length; i++){
			long pSize = partitionSizes.containsKey(partitions[i]) ? partitionSizes.get(partitions[i]) : 0;
			if(currentSize + pSize > maxSplitSize){
				splitId++;
				currentSize = 0;
			}
			currentSize += pSize;
			splitPartitionIds.put(splitId, partitions[i]);
		}
		
		for(Integer k: splitPartitionIds.keySet()){
			PartitionIterator itr = split.getIterator();
			if(itr instanceof RepartitionIterator)
				itr = ((RepartitionIterator)itr).createDistributedIterator();
			smallerSplits.add(new PartitionSplit(Ints.toArray(splitPartitionIds.get(k)), itr));
		}
		
		return smallerSplits;
	}
	
//	private List<PartitionSplit>
	
	
	
	
	public PartitionSplit[] resizeSplits(PartitionSplit[] initialSplits, Map<Integer,Long> partitionSizes, int maxSplitSize){

		//Map<Integer,Long> partitionSizes = getPartitionIdMap(initialSplits, partitionIdFileMap);
		List<PartitionSplit> resizedSplits = Lists.newArrayList();

		for(PartitionSplit split: initialSplits){
			int[] partitions = split.getPartitions();
			//if(partitions.length > maxSplitSize){
			if(getPartitionSplitSize(split,partitionSizes) > maxSplitSize){	
				// need to split the partition into smaller ones
				int from = 0; long currentSize = 0;
				for(int i=0;i<partitions.length;i++){
					long pSize = partitionSizes.get(partitions[i]);
					if(currentSize > 0 && currentSize + pSize > maxSplitSize){
						int[] subPartitions = Arrays.copyOfRange(partitions, from, i);

						PartitionIterator itr = split.getIterator();
						if(itr instanceof RepartitionIterator)
							itr = ((RepartitionIterator)itr).createDistributedIterator();

						resizedSplits.add(new PartitionSplit(subPartitions, itr));
						
						from = i;
						currentSize = 0;
					}
					currentSize += pSize;
				}
				
				if(from < partitions.length-1){	// the last split
					int[] subPartitions = Arrays.copyOfRange(partitions, from, partitions.length);
					PartitionIterator itr = split.getIterator();
					if(itr instanceof RepartitionIterator)
						itr = ((RepartitionIterator)itr).createDistributedIterator();
					resizedSplits.add(new PartitionSplit(subPartitions, itr));
				}
				
				
//				for(int i=0;i<partitions.length;i+=maxSplitSize){
//					int to = i + maxSplitSize > partitions.length ? partitions.length : i + maxSplitSize;
//					int[] subPartitions = Arrays.copyOfRange(partitions, i, to);
//
//					PartitionIterator itr = split.getIterator();
//					if(itr instanceof RepartitionIterator)
//						itr = ((RepartitionIterator)itr).createDistributedIterator();
//
//					resizedSplits.add(new PartitionSplit(subPartitions, itr));
//				}
			}
			else{
				PartitionIterator itr = split.getIterator();
				if(itr instanceof RepartitionIterator)
					itr = ((RepartitionIterator)itr).createDistributedIterator();
				resizedSplits.add(new PartitionSplit(split.getPartitions(), itr));
				resizedSplits.add(split);
			}
		}

		return resizedSplits.toArray(new PartitionSplit[resizedSplits.size()]);
	}

	public PartitionSplit[] combineSplits(PartitionSplit[] initialSplits, int minSplitSize, int maxSplitSize) {
		List<PartitionSplit> resizedSplits = Lists.newArrayList();
		List<PartitionSplit> smallSplit = Lists.newArrayList();
		int smallSplitSize = 0;

		for(PartitionSplit split: initialSplits) {
			int[] partitions = split.getPartitions();
			if (smallSplitSize + partitions.length > maxSplitSize) {
				int[] mergedIds = new int[0];
				for (PartitionSplit s : smallSplit) {
					mergedIds = Ints.concat(mergedIds, s.getPartitions());
				}
				PartitionSplit newSplit = new PartitionSplit(mergedIds, smallSplit.get(0).getIterator());
				resizedSplits.add(newSplit);
				smallSplit = Lists.newArrayList();
				smallSplitSize = 0;
			}

			if (split.getIterator().getClass() == RepartitionIterator.class && partitions.length <= minSplitSize) {
				smallSplit.add(split);
				smallSplitSize += partitions.length;
			} else {
				resizedSplits.add(split);
			}

		}

		if (smallSplit.size() > 0) {
			int[] mergedIds = new int[0];
			for (PartitionSplit s : smallSplit) {
				mergedIds = Ints.concat(mergedIds, s.getPartitions());
			}
			PartitionSplit newSplit = new PartitionSplit(mergedIds, smallSplit.get(0).getIterator());
			resizedSplits.add(newSplit);
		}

		return resizedSplits.toArray(new PartitionSplit[resizedSplits.size()]);
	}

	@Override
	public RecordReader<LongWritable, IteratorRecord> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new SparkRecordReader();
	}
}
