package core.index.build;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import core.data.CartilageDatum.CartilageFile;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.utils.BinaryUtils;
import core.utils.IOUtils;

public class InputReader {

	int bufferSize = 5 * 1024 * 1024;
	int blockSampleSize = 5 * 1024;
	char newLine = '\n';

	byte[] byteArray, brokenLine;
	ByteBuffer bb;
	int nRead, byteArrayIdx, previous;
	boolean hasLeftover;

	int totalLineSize, lineCount;
	long arrayCopyTime, bucketIdTime, brokenTime, clearTime;

	MDIndex index;
	CartilageIndexKey key;

	public boolean firstPass;

	public InputReader(MDIndex index, CartilageIndexKey key){
		this.index = index;
		this.key = key;
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

	public void scan(String filename, PartitionWriter writer){
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
				processByteBuffer(writer);
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

	/**
	 * Picks a block with samplingRate probability
	 * @param filename
	 * @param samplingRate
	 */
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

	private void processByteBuffer(PartitionWriter writer){
		long startTime;
		for ( ; byteArrayIdx<nRead; byteArrayIdx++ ){
	    	if(byteArray[byteArrayIdx]==newLine){

	    		totalLineSize += byteArrayIdx-previous;
	    		if(hasLeftover){
	    			startTime = System.nanoTime();
	    			byte[] a = new byte[brokenLine.length + byteArrayIdx-previous];
	    			System.arraycopy(brokenLine, 0, a, 0, brokenLine.length);
	    			System.arraycopy(byteArray, previous, a, brokenLine.length, byteArrayIdx-previous);
	    			key.setBytes(a);
	    			arrayCopyTime += System.nanoTime() - startTime;

	    			totalLineSize += brokenLine.length;
	    			hasLeftover = false;

	    			if(writer!=null){
						startTime = System.nanoTime();
						String bucketId = index.getBucketId(key).toString();
						bucketIdTime += System.nanoTime() - startTime;
	    				writer.writeToPartition(bucketId, a, 0, a.length);
	    			}
				} else {
					key.setBytes(byteArray, previous, byteArrayIdx-previous);
	    			if(writer!=null){
						startTime = System.nanoTime();
						String bucketId = index.getBucketId(key).toString();
						bucketIdTime += System.nanoTime() - startTime;
	    				writer.writeToPartition(bucketId, byteArray, previous, byteArrayIdx-previous);
	    			}
	    		}

	    		previous = ++byteArrayIdx;

	    		lineCount++;

	    		if(firstPass)
	    			index.insert(key);
	    	}
	    }
	}

	public static void main(String[] args) {
		java.nio.file.Path file = Paths.get("/Users/anil/Dev/repos/tpch-dbgen/lineitem.tbl");
		long t = System.nanoTime();
		if(Files.exists(file) && Files.isReadable(file)) {
		    try {
		        // File reader
		        BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset());

		        String line;
		        long count = 0;
		        // read each line
		        while((line = reader.readLine()) != null) {
		        	count += line.length();
		        }
		        reader.close();
		        System.out.println("String length is : " + count);
		    } catch (Exception e) {
		        e.printStackTrace();
		    }
		}
		System.out.println("SCAN: Total Time taken = "+(System.nanoTime()-t)/1E9+" sec");
	}
}
