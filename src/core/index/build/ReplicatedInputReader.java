package core.index.build;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Scanner;

import core.access.Query;
import core.data.CartilageDatum.CartilageFile;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.utils.BinaryUtils;
import core.utils.HDFSUtils;
import core.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;

public class ReplicatedInputReader {

	int bufferSize = 5 * 1024 * 1024;
	int blockSampleSize = 5 * 1024;
	char newLine = '\n';
	
	byte[] byteArray, brokenLine;
	ByteBuffer bb;
	int nRead, byteArrayIdx, previous;	
	boolean hasLeftover;
	
	int totalLineSize, lineCount;
	long arrayCopyTime, bucketIdTime, brokenTime, clearTime;
	
	MDIndex[] indexes;
	CartilageIndexKey[] keys;
	
	boolean firstPass;
	
	public ReplicatedInputReader(MDIndex[] indexes, CartilageIndexKey[] keys){
		this.indexes = indexes;
		this.keys = keys;
		this.firstPass = true;
		arrayCopyTime = 0;
		bucketIdTime = 0;
	}
	
	
	private void initScan(int bufferSize){
		byteArray = new byte[bufferSize];
		brokenLine = null;
		bb = ByteBuffer.wrap(byteArray);
		nRead=0; byteArrayIdx=0; previous=0;	
		hasLeftover = false;
		
		totalLineSize = 0;
		lineCount = 0;
		arrayCopyTime = 0;
		bucketIdTime = 0;
		brokenTime = 0;
		clearTime = 0;
	}
	
	public void scan(String filename){
		scan(filename, null);
	}
	
	public void scan(String filename, PartitionWriter[] writers){
		initScan(bufferSize);
		long sStartTime = System.nanoTime(), temp1;
		long readTime=0, processTime=0;
		FileChannel ch = IOUtils.openFileChannel(new CartilageFile(filename));
		int counter = 0;
		try {
			while (true) {
				temp1 = System.nanoTime();
				boolean allGood = ((nRead = ch.read(bb)) != -1);
				readTime += System.nanoTime() - temp1;

				if (!allGood)
					break;

				if(nRead==0)
					continue;


				counter++;

				byteArrayIdx = previous = 0;
				temp1 = System.nanoTime();
				processByteBuffer(writers);
				processTime += System.nanoTime() - temp1;

				long startTime = System.nanoTime();
				if(previous < nRead){	// is there a broken line in the end?
					brokenLine = BinaryUtils.getBytes(byteArray, previous, nRead-previous);
					hasLeftover = true;
				}
				brokenTime += System.nanoTime() - startTime;

				startTime = System.nanoTime();
				bb.clear();
				clearTime += System.nanoTime() - startTime;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		IOUtils.closeFileChannel(ch);
		firstPass = false;

		System.out.println("counter:" + counter);
		System.out.println("SCAN: Total Time taken = "+(System.nanoTime()-sStartTime)/1E9+" sec");
		System.out.println("Line count = " + lineCount);
		System.out.println("Average line size = " + (double) totalLineSize / lineCount);
		System.out.println("SCAN: Read into buffer time = " + readTime/1E9);
		System.out.println("SCAN: Process buffer time = " + processTime/1E9);

		System.out.println("SCAN: Array copy time = " + arrayCopyTime/1E9);
		System.out.println("SCAN: Get bucket ID time = " + bucketIdTime/1E9);
		System.out.println("SCAN: Broken line fix time = " + brokenTime/1E9);
		System.out.println("SCAN: Buffer clear time = " + clearTime/1E9);
	}

	public void scanWithBlockSampling(String filename, double samplingRate) {
		initScan(blockSampleSize);

		FileChannel ch = IOUtils.openFileChannel(new CartilageFile(filename));
		try {
			long position = 0;
			while (Math.random() > samplingRate) {
				position += blockSampleSize;
			}
			ch.position(position);
			while ((nRead = ch.read(bb)) != -1) {
				if(nRead==0)
					continue;

				byteArrayIdx = previous = 0;
				while (byteArrayIdx < nRead && byteArray[byteArrayIdx] != newLine) {
					byteArrayIdx++;
				}
				previous = ++byteArrayIdx;
				processByteBuffer(null);

				bb.clear();

				while (Math.random() > samplingRate) {
					position += blockSampleSize;
				}
				ch.position(position);
			};
		} catch (IOException e) {
			e.printStackTrace();
		}
		IOUtils.closeFileChannel(ch);
		firstPass = false;
	}
	
	private void processByteBuffer(PartitionWriter[] writers){
		long startTime;
		for ( ; byteArrayIdx<nRead; byteArrayIdx++ ){
	    	if(byteArray[byteArrayIdx]==newLine){
	    		
	    		totalLineSize += byteArrayIdx-previous;
	    		if(hasLeftover){
					startTime = System.nanoTime();
	    			byte[] a = new byte[brokenLine.length + byteArrayIdx-previous];
	    			System.arraycopy(brokenLine, 0, a, 0, brokenLine.length);
	    			System.arraycopy(byteArray, previous, a, brokenLine.length, byteArrayIdx-previous);
					arrayCopyTime += System.nanoTime() - startTime;
	    			
	    			totalLineSize += brokenLine.length;
	    			hasLeftover = false;
	    			
	    			for(int i=0; i<indexes.length; i++){
	    				keys[i].setBytes(a);
						if(writers!=null) {
							startTime = System.nanoTime();
							String bucketId = indexes[i].getBucketId(keys[i]).toString();
							bucketIdTime += System.nanoTime() - startTime;
							writers[i].writeToPartition(bucketId, a, 0, a.length);
						}
	    			}
	    		}
	    		else{
	    			for(int i=0; i<indexes.length; i++){
		    			keys[i].setBytes(byteArray, previous, byteArrayIdx-previous);
						if(writers!=null) {
							startTime = System.nanoTime();
							String bucketId = indexes[i].getBucketId(keys[i]).toString();
							bucketIdTime += System.nanoTime() - startTime;
							writers[i].writeToPartition(bucketId,  byteArray, previous, byteArrayIdx-previous);
						}
	    			}
	    		}

	    		previous = ++byteArrayIdx;
	    		
	    		lineCount++;
	    		
	    		if(firstPass)
	    			for(int i=0;i<indexes.length;i++)
	    				indexes[i].insert(keys[i]);
	    	}
	    }
	}

	public void scanSample(String sample) {
		Scanner sc = new Scanner(sample);
		sc.nextLine(); // first line of sample is types
		while (sc.hasNextLine()) {
			String record = sc.nextLine();
			for(int i=0;i<indexes.length;i++) {
				if (record.getBytes().length == 0) {
					continue;
				}
				keys[i].setBytes(record.getBytes());
				indexes[i].insert(keys[i]);
			}
		}
		sc.close();
	}
}
