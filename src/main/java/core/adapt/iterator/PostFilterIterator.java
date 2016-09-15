package core.adapt.iterator;

import com.google.common.io.ByteStreams;
import core.adapt.Query;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class PostFilterIterator extends PartitionIterator implements
        Serializable {
    private static final long serialVersionUID = 1L;

    public PostFilterIterator() {

    }

    public PostFilterIterator(Query q) {
        super(q);
    }

    public PostFilterIterator(String iteratorString) {
        try {
            readFields(ByteStreams.newDataInput(iteratorString.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read the fields");
        }
    }

    public static PostFilterIterator read(DataInput in) throws IOException {
        PostFilterIterator it = new PostFilterIterator();
        it.readFields(in);
        return it;
    }

    @Override
    protected boolean isRelevant(IteratorRecord record) {
        // return query.qualifies(record);
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
}
