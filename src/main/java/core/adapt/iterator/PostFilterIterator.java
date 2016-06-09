package core.adapt.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.io.ByteStreams;

import core.adapt.Query;
import org.apache.hadoop.io.Text;

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

	@Override
	protected boolean isRelevant(IteratorRecord record) {
		// return query.qualifies(record);
		if (query.qualifies(record)) {
			System.out.println("Extended Price " + record.getDoubleAttribute(2));
			return true;
		}
		return false;
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
