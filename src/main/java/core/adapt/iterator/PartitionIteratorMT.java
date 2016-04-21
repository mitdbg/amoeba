package core.adapt.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import core.adapt.Query;
import core.common.globals.TableInfo;
import core.utils.Pair;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import core.adapt.Partition;
import core.common.globals.Globals;
import core.utils.ReflectionUtils;

/**
 * Implements an iterator which processes just one file in its
 * entirety.
 */
public class PartitionIteratorMT implements Iterator<IteratorRecord> {

    public enum ITERATOR {
        SCAN, FILTER, REPART
    };

    protected IteratorRecord record;

    protected static char newLine = '\n';

    protected byte[] bytes;
    protected int bytesLength, offset, previous;

    // Stores the offset, length of the matching tuples.
    List<Pair<Integer, Integer>> results;
    int resultsIndex;

    protected Partition partition;

    protected Query query;

    public PartitionIteratorMT() {

    }

    // Assumes that the tableInfo is already loaded.
    public PartitionIteratorMT(Query q, Partition partition) {
        this.query = q;
        this.partition = partition;
        TableInfo tableInfo = Globals.getTableInfo(query.getTable());
        assert tableInfo != null;

        record = new IteratorRecord(tableInfo.delimiter);
        bytes = partition.getNextBytes();
        bytesLength = bytes == null ? 0 : bytes.length;
        offset = 0;
        previous = 0;

        results = new ArrayList<Pair<Integer, Integer>>();
        resultsIndex = 0;
    }

    public void processRecord() {
        try {
            record.setBytes(bytes, previous, offset - previous);
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Index out of bounds while setting bytes: "
                    + (new String(bytes, previous, offset - previous)));
            throw e;
        }

        previous = ++offset;
        if (isRelevant(record)) {
            results.add(new Pair<>(previous, offset));
        }
    }

    public void compute() {
        for (; offset < bytesLength; offset++) {
            if (bytes[offset] == newLine) {
               processRecord();
            }
        }

        if (previous < bytesLength) {
            processRecord();
        }
    }

    @Override
    public boolean hasNext() {
        return resultsIndex < results.size();
    }

    protected boolean isRelevant(IteratorRecord record) {
        return true;
    }

    @Override
    public void remove() {
        next();
    }

    @Override
    public IteratorRecord next() {
        Pair<Integer, Integer> p = results.get(resultsIndex);
        record.setBytes(bytes, p.first, p.second);
        resultsIndex++;
        return record;
    }

    public void finish() {
    }

    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    // public static PartitionIterator read(DataInput in) throws IOException {
    // PartitionIterator it = new PartitionIterator();
    // it.readFields(in);
    // return it;
    // }

    public static String iteratorToString(PartitionIterator itr) {
        ByteArrayDataOutput dat = ByteStreams.newDataOutput();
        try {
            itr.write(dat);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialize the partitioner");
        }
        return itr.getClass().getName() + "@" + new String(dat.toByteArray());
    }

    public static PartitionIterator stringToIterator(String itrString) {
        String[] tokens = itrString.split("@", 2);
        return (PartitionIterator) ReflectionUtils.getInstance(tokens[0],
                new Class<?>[] { String.class }, new Object[] { tokens[1] });
    }
}
