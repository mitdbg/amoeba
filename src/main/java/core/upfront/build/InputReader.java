package core.upfront.build;

import core.common.index.MDIndex;
import core.common.key.RawIndexKey;
import core.utils.BinaryUtils;
import core.utils.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class InputReader {

    public boolean firstPass;
    int bufferSize = 5 * 1024 * 1024;
    int blockSampleSize = 50 * 1024;
    char newLine = '\n';
    byte[] byteArray, brokenLine;
    ByteBuffer bb;
    int nRead, byteArrayIdx, previous;
    boolean hasLeftover;
    int totalLineSize, lineCount;
    long arrayCopyTime, bucketIdTime, brokenTime, clearTime;
    MDIndex index;
    RawIndexKey key;

    public InputReader(MDIndex index, RawIndexKey key) {
        this.index = index;
        this.key = key;
        this.firstPass = true;
        arrayCopyTime = 0;
        bucketIdTime = 0;
    }

    private void initScan(int bufferSize) {
        byteArray = new byte[bufferSize];
        brokenLine = null;
        bb = ByteBuffer.wrap(byteArray);
        nRead = 0;
        byteArrayIdx = 0;
        previous = 0;
        hasLeftover = false;

        totalLineSize = 0;
        lineCount = 0;
        arrayCopyTime = 0;
        bucketIdTime = 0;
        brokenTime = 0;
        clearTime = 0;
    }

    /**
     * Does a full scan over entire data.
     * Used in second phase of up-front partitioning.
     *
     * @param filename
     * @param writer
     */
    public void scan(String filename, PartitionWriter writer) {
        initScan(bufferSize);
        long sStartTime = System.nanoTime(), temp1;
        long readTime = 0, processTime = 0;
        FileChannel ch = IOUtils.openFileChannel(filename);
        int counter = 0;
        try {
            while (true) {
                temp1 = System.nanoTime();
                boolean allGood = ((nRead = ch.read(bb)) != -1);
                readTime += System.nanoTime() - temp1;

                if (!allGood)
                    break;

                if (nRead == 0)
                    continue;

                counter++;

                byteArrayIdx = previous = 0;
                temp1 = System.nanoTime();
                processByteBuffer(writer, null);
                processTime += System.nanoTime() - temp1;

                long startTime = System.nanoTime();
                if (previous < nRead) { // is there a broken line in the end?
                    brokenLine = BinaryUtils.getBytes(byteArray, previous, nRead - previous);
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
        System.out.println("SCAN: Total Time taken = " + (System.nanoTime() - sStartTime) / 1E9 + " sec");
        System.out.println("Line count = " + lineCount);
        System.out.println("Average line size = " + (double) totalLineSize / lineCount);
        System.out.println("SCAN: Read into buffer time = " + readTime / 1E9);
        System.out.println("SCAN: Process buffer time = " + processTime / 1E9);

        System.out.println("SCAN: Array copy time = " + arrayCopyTime / 1E9);
        System.out.println("SCAN: Get bucket ID time = " + bucketIdTime / 1E9);
        System.out.println("SCAN: Broken line fix time = " + brokenTime / 1E9);
        System.out.println("SCAN: Buffer clear time = " + clearTime / 1E9);
    }

    /**
     * Return true at the probability of samplingRate.
     *
     * @param samplingRate
     */

    private boolean sampleSucceed(double samplingRate) {
        return Math.random() <= samplingRate;
    }

    /**
     * Picks a block with samplingRate probability
     * Used in the first phase of up-front partitioning.
     *
     * @param filename
     * @param samplingRate
     */
    public void scanWithBlockSampling(String filename, double samplingRate, OutputStream out) {
        initScan(blockSampleSize);
        FileChannel ch = IOUtils.openFileChannel(filename);
        try {
            for (long position = 0; ; position += blockSampleSize) {
                while (sampleSucceed(samplingRate) == false) {
                    position += blockSampleSize;
                }
                ch.position(position);
                if ((nRead = ch.read(bb)) == -1) {
                    break;
                }

                byteArrayIdx = previous = 0;
                if (position != 0) {
                    // skip the first tuple if not starting of file.
                    while (byteArrayIdx < nRead && byteArray[byteArrayIdx] != newLine) {
                        byteArrayIdx++;
                    }
                    previous = ++byteArrayIdx;
                }

                processByteBuffer(null, out);
                bb.clear();
                out.flush(); // It only helps get an exact profiling?
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        IOUtils.closeFileChannel(ch);
        firstPass = false;
    }

    private void processByteBuffer(PartitionWriter writer, OutputStream out) {
        long startTime;
        try {
            for (; byteArrayIdx < nRead; byteArrayIdx++) {
                if (byteArray[byteArrayIdx] == newLine) {
                    totalLineSize += byteArrayIdx - previous;
                    if (hasLeftover) {
                        startTime = System.nanoTime();
                        byte[] keyBytes = new byte[brokenLine.length + byteArrayIdx - previous + 1];
                        System.arraycopy(brokenLine, 0, keyBytes, 0, brokenLine.length);
                        System.arraycopy(byteArray, previous, keyBytes, brokenLine.length, byteArrayIdx - previous + 1); // +
                        arrayCopyTime += System.nanoTime() - startTime;
                        totalLineSize += brokenLine.length;
                        hasLeftover = false;

                        if (out != null) {
                            out.write(keyBytes);
                        }

                        if (writer != null) {
                            key.setBytes(keyBytes, 0, keyBytes.length - 1); // // skip newline
                            startTime = System.nanoTime();
                            String bucketId = index.getBucketId(key).toString();
                            bucketIdTime += System.nanoTime() - startTime;
                            writer.writeToPartition(bucketId, keyBytes, 0, keyBytes.length);
                        }
                    } else {
                        if (out != null) {
                            out.write(byteArray, previous, byteArrayIdx - previous + 1); // + 1 newline
                        }

                        if (writer != null) {
                            key.setBytes(byteArray, previous, byteArrayIdx - previous); // skip newline
                            startTime = System.nanoTime();
                            String bucketId = index.getBucketId(key).toString();
                            bucketIdTime += System.nanoTime() - startTime;
                            writer.writeToPartition(bucketId, byteArray, previous, byteArrayIdx - previous + 1);
                        }
                    }
                    previous = ++byteArrayIdx;
                    lineCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
