package core.adapt;

import core.utils.BinaryUtils;
import core.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.Serializable;

public class Partition implements Cloneable, Serializable {

    private static final long serialVersionUID = 1L;

    private static int V_MAX_INT = 1024 * 1024 * 50;
    protected String path;

    ;
    protected byte[] bytes;
    protected int offset;
    protected int recordCount;
    protected int partitionId;
    protected boolean nextBytesReturned = false;
    State state;

    /**
     * Create an existing partition object.
     *
     * @param pathAndPartitionId
     */
    public Partition(String pathAndPartitionId) {
        this(FilenameUtils.getPath(pathAndPartitionId), Integer
                .parseInt(FilenameUtils.getBaseName(pathAndPartitionId)));
    }

    public Partition(String path, int partitionId) {
        this.path = path;
        this.partitionId = partitionId;
        this.state = State.ORIG;
        this.offset = 0;
        this.recordCount = 0;
    }

    @Override
    public Partition clone() {
        Partition p = new Partition(path, partitionId);
        p.bytes = new byte[bytes.length];
        p.state = State.NEW;
        return p;
    }

    public void setPathAndPartitionId(String pathAndPartitionId) {
        this.path = FilenameUtils.getPath(pathAndPartitionId);
        this.partitionId = Integer.parseInt(FilenameUtils
                .getBaseName(pathAndPartitionId));
        this.offset = 0;
    }

    public int getPartitionId() {
        return this.partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public String getPath() {
        return path;
    }

    public int size() {
        return offset;
    }

    public boolean load() {
        if (path == null || path.equals(""))
            return false;
        if (!path.startsWith("/"))
            path = "/" + path;
        bytes = IOUtils.readByteArray(path + "/" + partitionId);
        offset = bytes.length;
        return true; // load the physical block for this partition
    }

    public void write(byte[] source, int offset, int length) {
        while (this.offset + length >= bytes.length) {
            if (bytes.length >= V_MAX_INT) {
                store(true);
                this.offset = 0;
            } else
                bytes = BinaryUtils.resize(bytes, (int) (bytes.length * 1.5));
        }

        if (offset + length > source.length)
            System.out.println("something is wrong!");

        try {
            System.arraycopy(source, offset, bytes, this.offset, length);
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
            System.out.println(new String(bytes) + " " + source.length + " "
                    + offset + " " + this.offset + " " + length + " "
                    + bytes.length);
            throw e;
        }
        this.offset += length;
        bytes[this.offset] = '\n';
        this.offset++;
        this.recordCount++;
    }

    public void store(boolean append) {
        String storePath = path + "/" + partitionId;
        IOUtils.writeByteArray(storePath, bytes, 0, offset, append);
    }

    public void drop() {
        FileUtils.deleteQuietly(new File(path));
    }

    public byte[] getBytes() {
        if (bytes == null)
            load();
        return bytes;
    }

    public byte[] getNextBytes() {
        byte[] r = nextBytesReturned ? null : getBytes();
        nextBytesReturned = true;
        return r;
    }

    @Override
    public boolean equals(Object p) {
        return ((Partition) p).path.equals(path)
                && ((Partition) p).partitionId == partitionId;
    }

    @Override
    public int hashCode() {
        return (path + partitionId).hashCode();
    }

    public int getRecordCount() {
        return this.recordCount; // returns the number of records written to
        // this partition
    }

    public static enum State {
        ORIG, NEW
    }
}
