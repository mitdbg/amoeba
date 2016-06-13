package core.upfront.build;

import core.common.index.MDIndex;
import core.common.key.RawIndexKey;
import core.utils.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InputReaderMT {

    int bufferSize = 256 * 1024 * 16;
    char newLine = '\n';

    byte[] byteArray, brokenLine;
    ByteBuffer bb;
    int nRead;
    int lineCount;

    MDIndex index;
    RawIndexKey[] keys;

    boolean firstPass;

    public InputReaderMT(MDIndex index, RawIndexKey[] keys) {
        this.index = index;
        this.keys = keys;
        this.firstPass = true;
    }

    private void initScan() {
        byteArray = new byte[bufferSize];
        brokenLine = null;
        bb = ByteBuffer.wrap(byteArray);
        nRead = 0;
        lineCount = 0;
    }

    public void scan(String filename, int numThreads) {
        scan(filename, null, numThreads);
    }

    public void scan(String filename, PartitionWriter writer, int numThreads) {
        initScan();

        BufferProcessor[] buffProcessors = new BufferProcessor[numThreads];
        for (int i = 0; i < buffProcessors.length; i++)
            buffProcessors[i] = new BufferProcessor(keys[i], writer);

        FileChannel ch = IOUtils.openFileChannel(filename);
        try {
            while ((nRead = ch.read(bb)) != -1) {
                if (nRead == 0)
                    continue;

                // launch parallel threads
                ExecutorService executor = Executors
                        .newFixedThreadPool(numThreads);
                int bytesPerThread = nRead / numThreads;
                for (int i = 0; i < buffProcessors.length; i++) {
                    if (i < buffProcessors.length - 1)
                        buffProcessors[i].init(i * bytesPerThread, (i + 1)
                                * bytesPerThread);
                    else
                        buffProcessors[i].init(i * bytesPerThread, nRead);
                    executor.execute(buffProcessors[i]);
                }
                executor.shutdown();
                // Wait until all threads are finish
                while (!executor.isTerminated())
                    ;

                // tie together broken records from all threads
                for (int i = 0; i < buffProcessors.length; i++) {
                    byte[] a;
                    byte[] x = buffProcessors[i].getFirstBroken();
                    if (brokenLine == null)
                        a = x;
                    else {
                        a = new byte[brokenLine.length + x.length];
                        System.arraycopy(brokenLine, 0, a, 0, brokenLine.length);
                        System.arraycopy(x, 0, a, brokenLine.length, x.length);
                    }
                    keys[0].setBytes(a);
                    if (writer != null)
                        writer.writeToPartition(index.getBucketId(keys[0])
                                .toString(), a, 0, a.length);
                    // if(firstPass)
                    // index.insert(keys[0]);
                    brokenLine = buffProcessors[i].getLastBroken();
                    lineCount += buffProcessors[i].getLineCount();
                }

                bb.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        IOUtils.closeFileChannel(ch);
        firstPass = false;

        // System.out.println("Time taken = "+(double)(System.nanoTime()-startTime)/1E9+" sec");
        System.out.println("Line count = " + lineCount);
    }

    public class BufferProcessor implements Runnable {
        PartitionWriter writer;
        private RawIndexKey key;
        private int curr, previous, end;
        private int lineCount;
        private byte[] firstBroken, lastBroken;

        /**
         * @param start -- including
         * @param end   -- excluding
         */
        public BufferProcessor(RawIndexKey key, PartitionWriter writer) {
            this.key = key;
            this.writer = writer;
        }

        public void init(int start, int end) {
            this.curr = start;
            this.previous = start;
            this.end = end;
            this.lineCount = 0;
            this.firstBroken = null;
            this.lastBroken = null;
        }

        public void run() {
            for (; curr < end; curr++) {
                if (byteArray[curr] == newLine) {
                    if (lineCount == 0) {
                        firstBroken = new byte[curr - previous];
                        System.arraycopy(byteArray, previous, firstBroken, 0,
                                firstBroken.length);
                    } else {
                        key.setBytes(byteArray, previous, curr - previous);
                        if (writer != null)
                            writer.writeToPartition(index.getBucketId(key)
                                    .toString(), byteArray, previous, curr
                                    - previous);
                        // if(firstPass)
                        // index.insert(key);
                    }
                    previous = ++curr;
                    lineCount++;
                }
            }
            if (previous < end) {
                lastBroken = new byte[end - previous];
                System.arraycopy(byteArray, previous, lastBroken, 0,
                        lastBroken.length);
            }
        }

        public int getLineCount() {
            return this.lineCount;
        }

        public byte[] getFirstBroken() {
            return this.firstBroken;
        }

        public byte[] getLastBroken() {
            return this.lastBroken;
        }
    }
}
