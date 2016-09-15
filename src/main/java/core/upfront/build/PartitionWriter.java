package core.upfront.build;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class PartitionWriter implements Cloneable {
    protected int bufferPartitionSize = 2 * 1024 * 1024;  // 2 MB
    protected String partitionDir;

    protected Map<String, OutputStream> buffer;
    protected Map<String, MutableInt> partitionRecordCount;
    private long writingTime = 0;

    public PartitionWriter(String partitionDir, int bufferPartitionSize) {
        this(partitionDir);
        this.bufferPartitionSize = bufferPartitionSize;
    }

    public PartitionWriter(String partitionDir) {
        this.partitionDir = partitionDir;
        this.buffer = Maps.newHashMap();
        this.partitionRecordCount = Maps.newHashMap();
    }

    @Override
    public PartitionWriter clone() throws CloneNotSupportedException {
        PartitionWriter w = (PartitionWriter) super.clone();
        w.buffer = Maps.newHashMap();
        w.partitionRecordCount = Maps.newHashMap();
        return w;
    }

    public void writeToPartition(String partitionId, byte[] bytes,
                                 int b_offset, int b_length) {
        long start = System.nanoTime();
        OutputStream b = buffer.get(partitionId);
        if (b == null) {
            b = getOutputStream(partitionDir + "/" + partitionId);
            buffer.put(partitionId, b);
        }

        try {
            b.write(bytes, b_offset, b_length);
            writingTime += System.nanoTime() - start;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected abstract OutputStream getOutputStream(String path);

    public abstract void createPartitionDir();

    public void flush() {
        flush(buffer.size());
    }

    protected void flush(int numPartitions) {
        System.out.println("SCAN: partition writing time = " + writingTime
                / 1E9);
        long start = System.nanoTime();
        int flushCount = 0;
        Set<String> keys = new HashSet<String>(buffer.keySet());
        for (String k : keys) {
            try {
                buffer.get(k).close();
                buffer.remove(k);
            } catch (IOException e) {
                e.printStackTrace();
            }
            flushCount++;
            if (flushCount > numPartitions)
                break;
        }
        System.out.println("SCAN: flushing time = "
                + (System.nanoTime() - start) / 1E9);
    }
}
