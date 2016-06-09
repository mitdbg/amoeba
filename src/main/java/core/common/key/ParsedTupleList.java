package core.common.key;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import core.common.globals.Globals;
import core.utils.BinaryUtils;
import core.utils.Pair;
import core.utils.TypeUtils;
import core.utils.TypeUtils.SimpleDate;
import core.utils.TypeUtils.TYPE;

/**
 * A class to collect a set of keys during index building. Example use case is
 * to collect samples before inserting them into an index structure.
 *
 * @author alekh
 *
 */
public class ParsedTupleList {

	private List<Object[]> values;
	private TYPE[] types;
	private int sampleSize;

    public ParsedTupleList(TYPE[] types) {
        this.types = types;
        this.values = Lists.newArrayList();
        sampleSize = 0;
    }

	/**
	 * Instantiate a key set with a given set of keys.
	 *
	 * @param values
	 */
	public ParsedTupleList(List<Object[]> values, TYPE[] types) {
		this.values = values;
		this.types = types;
		this.sampleSize = 0;
	}

	/**
	 * Insert into the key set.
	 *
	 * @param key
	 */
	public void insert(RawIndexKey key) {
		Object[] keyValues = new Object[types.length];
		for (int i = 0; i < types.length; i++) {
			switch (types[i]) {
			case INT:
				keyValues[i] = key.getIntAttribute(i);
				break;
			case LONG:
				keyValues[i] = key.getLongAttribute(i);
				break;
			case DOUBLE:
				keyValues[i] = key.getDoubleAttribute(i);
				break;
			case DATE:
				keyValues[i] = key.getDateAttribute(i, new SimpleDate(0, 0, 0));
				break;
			case STRING:
				keyValues[i] = key.getStringAttribute(i);
				break;
			case VARCHAR:
				break; // skip partitioning on varchar attribute
			default:
				throw new RuntimeException("Unknown dimension type:");
			}
		}

		values.add(keyValues);
		sampleSize++;
	}

	/**
	 * Return the last entry in the keyset
	 */
	public Object getFirst(int dim) {
		assert values.size() > 0;
		assert dim < types.length;
		return values.get(0)[dim];
	}

	/**
	 * Return the last entry in the keyset
	 */
	public Object getLast(int dim) {
		assert values.size() > 0;
		assert dim < types.length;
		return values.get(values.size() - 1)[dim];
	}

	public Object[] getCutpoints(int dim, int numParts) {
		Object[] cutpoints = new Object[numParts + 1];
		this.sort(dim);
		cutpoints[0] = this.getFirst(dim);
		cutpoints[numParts] = this.getLast(dim);
		int partLength = this.values.size() / numParts;
		for (int i = 1; i <= numParts - 1; i++) {
			cutpoints[i] = this.values.get(i * partLength)[dim];
		}
		return cutpoints;
	}

	public Comparator<Object[]> getComparatorForType(TYPE type,
			final int attributeIdx) {
		switch (type) {
		case INT:
			return new Comparator<Object[]>() {
				@Override
				public int compare(Object[] o1, Object[] o2) {
					return ((Integer) o1[attributeIdx])
							.compareTo((Integer) o2[attributeIdx]);
				}
			};
		case LONG:
			return new Comparator<Object[]>() {
				@Override
				public int compare(Object[] o1, Object[] o2) {
					return ((Long) o1[attributeIdx])
							.compareTo((Long) o2[attributeIdx]);
				}
			};
		case DOUBLE:
			return new Comparator<Object[]>() {
				@Override
				public int compare(Object[] o1, Object[] o2) {
					return ((Double) o1[attributeIdx])
							.compareTo((Double) o2[attributeIdx]);
				}
			};
		case DATE:
			return new Comparator<Object[]>() {
				@Override
				public int compare(Object[] o1, Object[] o2) {
					return ((SimpleDate) o1[attributeIdx])
							.compareTo((SimpleDate) o2[attributeIdx]);
				}
			};
		case STRING:
			return new Comparator<Object[]>() {
				@Override
				public int compare(Object[] o1, Object[] o2) {
					return ((String) o1[attributeIdx])
							.compareTo((String) o2[attributeIdx]);
				}
			};
		case VARCHAR:
			throw new RuntimeException("sorting over varchar is not supported"); // skip
																					// partitioning
																					// on
																					// varchar
																					// attribute
		default:
			throw new RuntimeException("Unknown dimension type: " + type);
		}
	}

	/**
	 * Sort the key set on the given attribute.
	 *
	 * @param attributeIdx
	 *            -- the index of the sort attribute relative to the key
	 *            attributes.
	 *
	 *            Example -- if we have 5 attributes ids (0,1,2,3,4) and we
	 *            extract 3 of them in the CartilageIndexKey, e.g. (1,3,4) and
	 *            now if we want to sort on attribute 3 then the attributeIdx
	 *            parameter will pass 1 (sorting on the second key attribute).
	 *
	 */
	public void sort(final int attributeIdx) {
		final TYPE sortType = types[attributeIdx];
		try {
			Collections.sort(values,
					this.getComparatorForType(sortType, attributeIdx));
		} catch (ClassCastException e) {
			System.out.println("EXCEPTION: " + attributeIdx + " "
					+ sortType.toString());
			e.printStackTrace();
		}
	}

	/**
	 * Split this key set into two equal sized key sets. If the length is odd,
	 * the extra value goes in the second set. This function assumes that the
	 * key set has already been sorted.
	 *
	 * Note that we do not copy the keys into a new object (we simply create a
	 * view using the subList() method). Therefore, the original key set must
	 * not be destroyed after the split.
	 *
	 * @return
	 */
	public Pair<ParsedTupleList, ParsedTupleList> splitInTwo() {
		ParsedTupleList k1 = new ParsedTupleList(values.subList(0,
				values.size() / 2), types);
		ParsedTupleList k2 = new ParsedTupleList(values.subList(
				values.size() / 2, values.size()), types);
		return new Pair<ParsedTupleList, ParsedTupleList>(k1, k2);
	}


	/**
	 * Split this key set by the median on the specified attribute. All keys
	 * with the specified attribute larger than the median go in the second set.
	 *
	 * @return
	 */
	public Pair<ParsedTupleList, ParsedTupleList> splitByMedianLarger(
			int attributeIdx) {
		Object medianVal = values.get(values.size() / 2)[attributeIdx];
		Comparator<Object> comp = TypeUtils.getComparatorForType(types[attributeIdx]);

		int lo = 0;
		int hi = values.size() - 1;
		int res = -1;
		while (lo <= hi) {
			int mid = (lo + hi) / 2;
			Object midVal = this.values.get(mid)[attributeIdx];
			try {
				if (comp.compare(midVal, medianVal) > 0) {
					res = mid;
					hi = mid - 1;
				}
				else
				{
					lo = mid + 1;
				}

			} catch (Exception e) {
				System.out.println(midVal.toString() + " " + medianVal.toString() + " " + attributeIdx + " " + medianVal.getClass().getSimpleName() + " " + midVal.getClass().getSimpleName());
				e.printStackTrace();
			}
		}

		ParsedTupleList k1 = new ParsedTupleList(values.subList(0,
				res), types);
		ParsedTupleList k2 = new ParsedTupleList(values.subList(
				res, values.size()), types);
		return new Pair<ParsedTupleList, ParsedTupleList>(k1, k2);
	}

	/**
	 * Split this key set by the median on the specified attribute. All keys
	 * with the specified attribute equal to the median go in the second set.
	 *
	 * @return
	 */
	public Pair<ParsedTupleList, ParsedTupleList> splitByMedian(
			int attributeIdx) {
		Object medianVal = values.get(values.size() / 2)[attributeIdx];
		Comparator<Object> comp = TypeUtils.getComparatorForType(types[attributeIdx]);

		int lo = 0;
		int hi = values.size() - 1;
		int res = -1;
		while (lo <= hi) {
			int mid = (lo + hi) / 2;
			Object midVal = this.values.get(mid)[attributeIdx];
			try {
				if (comp.compare(midVal, medianVal) >= 0) {
					res = mid;
					hi = mid - 1;
				}
				else
				{
					lo = mid + 1;
				}

			} catch (Exception e) {
				System.out.println(midVal.toString() + " " + medianVal.toString() + " " + attributeIdx + " " + medianVal.getClass().getSimpleName() + " " + midVal.getClass().getSimpleName());
				e.printStackTrace();
			}
		}

		ParsedTupleList k1 = new ParsedTupleList(values.subList(0,
				res), types);
		ParsedTupleList k2 = new ParsedTupleList(values.subList(
				res, values.size()), types);
		return new Pair<ParsedTupleList, ParsedTupleList>(k1, k2);
	}

	/**
	 * Split this key set by the value on the specified attribute.
	 *
	 * @return
	 */
	public Pair<ParsedTupleList, ParsedTupleList> splitAt(
			int attributeIdx, Object value) {
		final TYPE sortType = types[attributeIdx];

		Comparator<Object> comp = TypeUtils.getComparatorForType(sortType);

		// Finds the least k such that k > value
		int lo = 0;
		int hi = values.size() - 1;
		int res = values.size(); // assume every value is smaller than or equal to the given value
		while (lo <= hi) {
			int mid = (lo + hi) / 2;
			Object midVal = this.values.get(mid)[attributeIdx];
			try {
				if (comp.compare(midVal, value) > 0) {
					res = mid;
					hi = mid - 1;
				}
				else
				{
					lo = mid + 1;
				}

			} catch (Exception e) {
				System.out.println(midVal.toString() + " " + value.toString() + " " + attributeIdx + " " + value.getClass().getSimpleName() + " " + midVal.getClass().getSimpleName());
				e.printStackTrace();
			}
		}


		ParsedTupleList	k1 = new ParsedTupleList(values.subList(0, res), types);
		ParsedTupleList	k2 = new ParsedTupleList(values.subList(res, values.size()), types);

		return new Pair<ParsedTupleList, ParsedTupleList>(k1, k2);
	}

	/**
	 * Sort and then split the key set.
	 *
	 * @param attributeIdx
	 * @return
	 */
	public Pair<ParsedTupleList, ParsedTupleList> sortAndSplit(
			final int attributeIdx) {
		sort(attributeIdx);

		Pair<ParsedTupleList, ParsedTupleList> halves = splitByMedian(attributeIdx);

		if (halves.first.size() == 0 && !halves.second.getFirst(attributeIdx).equals(halves.second.getLast(attributeIdx))){
			halves = splitByMedianLarger(attributeIdx);
		}
		return halves;
	}

	public List<Object[]> getValues() {
		return values;
	}

	public void addValues(List<Object[]> e) {
		this.values.addAll(e);
	}

	public void setTypes(TYPE[] types) {
		this.types = types;
	}

	public TYPE[] getTypes() {
		return this.types;
	}

	public int size() {
		return values.size();
	}

	public void reset() {
		values = Lists.newArrayList();
		types = null;
	}

	/**
	 * Iterate over the keys in the key set.
	 *
	 * @return
	 */
	public KeySetIterator iterator() {
		return new KeySetIterator(values, types);
	}

	/**
	 * An iterator to iterate over the key in a key set. The Iterator returns an
	 * instance of ParsedIndexKey which extends CartilageIndexKey.
	 *
	 * @author alekh
	 *
	 */
	public static class KeySetIterator implements Iterator<ParsedIndexKey> {
		private Iterator<Object[]> valueItr;
		private ParsedIndexKey key;

		public KeySetIterator(List<Object[]> values, TYPE[] types) {
			this.valueItr = values.iterator();
			key = new ParsedIndexKey(types);
		}

		@Override
		public boolean hasNext() {
			return valueItr.hasNext();
		}

		@Override
		public ParsedIndexKey next() {
			key.setValues(valueItr.next());
			return key;
		}

		@Override
		public void remove() {
			next();
		}
	}

	public byte[] marshall(char delimiter) {
		List<byte[]> byteArrays = Lists.newArrayList();

		int initialSize = sampleSize * 400; // assumption: each record could be
											// of max size 400 bytes
		byte[] recordBytes = new byte[initialSize];

		int offset = 0;
		for (Object[] v : values) {
			byte[] vBytes = Joiner.on(delimiter).join(v).getBytes();
			if (offset + vBytes.length + 1 > recordBytes.length) {
				byteArrays.add(BinaryUtils.resize(recordBytes, offset));
				recordBytes = new byte[initialSize];
				offset = 0;
			}
			BinaryUtils.append(recordBytes, offset, vBytes);
			offset += vBytes.length;
			recordBytes[offset++] = '\n';
		}

		byteArrays.add(BinaryUtils.resize(recordBytes, offset));

		byte[][] finalByteArrays = new byte[byteArrays.size()][];
		for (int i = 0; i < finalByteArrays.length; i++)
			finalByteArrays[i] = byteArrays.get(i);

		return Bytes.concat(finalByteArrays);
	}

	public void unmarshall(byte[] bytes, char delimiter) {
		RawIndexKey record = new RawIndexKey(delimiter);
		int offset = 0, previous = 0;
		for (; offset < bytes.length; offset++) {
			if (bytes[offset] == '\n') {
				byte[] lineBytes = ArrayUtils.subarray(bytes, previous, offset);
				record.setBytes(lineBytes);
				try {
					insert(record);
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println("Exception in ParsedTupleList::unmarshall");
					e.printStackTrace();
				}
				previous = ++offset;
			}
		}
	}
}
