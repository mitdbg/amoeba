package core.adapt.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import core.adapt.Partition;
import core.adapt.Query;
import org.apache.hadoop.io.Text;

public class PostFilterIteratorMT extends PartitionIteratorMT implements
        Serializable {
    private static final long serialVersionUID = 1L;

    public PostFilterIteratorMT() {

    }

    public PostFilterIteratorMT(Query q, Partition p) {
        super(q, p);
    }

    @Override
    protected boolean isRelevant(IteratorRecord record) {
        return query.qualifies(record);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        query.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String predicateString = Text.readString(in);
        query = new Query(predicateString);
    }

    public static PostFilterIterator read(DataInput in) throws IOException {
        PostFilterIterator it = new PostFilterIterator();
        it.readFields(in);
        return it;
    }
}
