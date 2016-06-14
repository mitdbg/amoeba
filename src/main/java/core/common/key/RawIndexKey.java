package core.common.key;

import core.utils.TypeUtils.SimpleDate;

import java.io.IOException;
import java.io.Serializable;

public class RawIndexKey implements Cloneable, Serializable {

    protected transient byte[] bytes;
    protected transient int offset, length;
    protected transient int numAttrs;
    protected transient int[] attributeOffsets;

    protected char delimiter;
    private transient SimpleDate dummyDate = new SimpleDate(0, 0, 0);

    public RawIndexKey(char delimiter) {
        this.delimiter = delimiter;
    }

    public RawIndexKey(String keyString) {
        String[] tokens = keyString.trim().split(",");
        this.delimiter = tokens[0].charAt(0);
    }

    @Override
    public RawIndexKey clone() throws CloneNotSupportedException {
        RawIndexKey k = (RawIndexKey) super.clone();
        k.delimiter = this.delimiter;
        k.dummyDate = new SimpleDate(0, 0, 0);
        return k;
    }

    private void setNumAttrs(byte[] bytes, int offset, int length) {
        numAttrs = 1;
        for (int i = offset; i < offset + length; i++) {
            if (bytes[i] == delimiter) {
                numAttrs += 1;
            }
        }
    }


    public void setBytes(byte[] bytes) {
        setBytes(bytes, 0, bytes.length);
    }

    public void setBytes(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;

        if (attributeOffsets == null) {
            setNumAttrs(bytes, offset, length);
            attributeOffsets = new int[numAttrs];
        }

        int previous = offset;
        int attrIdx = 0;
        for (int i = offset; i < offset + length; i++) {
            if (bytes[i] == delimiter) {
                try {
                    attributeOffsets[attrIdx++] = previous;
                    previous = i + 1;
                } catch (Exception e) {
                    System.out.println(delimiter + " " + new String(bytes));
                    e.printStackTrace();
                }
            }
        }
        if (attrIdx < attributeOffsets.length)
            attributeOffsets[attrIdx] = previous;
    }

    public String getKeyString() {
        return new String(bytes, offset, length);
    }

    public String getStringAttribute(int index) {
        int off = attributeOffsets[index];
        int strSize;
        if (index < attributeOffsets.length - 1)
            strSize = attributeOffsets[index + 1] - off - 1;
        else
            strSize = offset + length - off;

        return new String(bytes, off, strSize);
    }

    public int getIntAttribute(int index) {
        int off = attributeOffsets[index];
        int len;
        if (index < attributeOffsets.length - 1)
            len = attributeOffsets[index + 1] - off - 1;
        else
            len = offset + length - off;

        int ret = Integer.parseInt(new String(bytes, off, len));

        return ret;
    }

    public char getDelimiter() {
        return this.delimiter;
    }

    public long getLongAttribute(int index) {
        int off = attributeOffsets[index];
        int len;
        if (index < attributeOffsets.length - 1)
            len = attributeOffsets[index + 1] - off - 1;
        else
            len = offset + length - off;

        long ret = Long.parseLong(new String(bytes, off, len));

        return ret;
    }

    public double getDoubleAttribute(int index) {
        int off = attributeOffsets[index];
        int len;
        if (index < attributeOffsets.length - 1)
            len = attributeOffsets[index + 1] - off - 1;
        else
            len = offset + length - off;

        double ret = Double.parseDouble(new String(bytes, off, len));
        return ret;
    }

    /*
     * Parse date assuming the format: "yyyy-MM-dd".
	 * Skips anything after that.
	 */
    public SimpleDate getDateAttribute(int index) {
        int off = attributeOffsets[index];
        int year = 1000 * (bytes[off] - '0') + 100 * (bytes[off + 1] - '0')
                + 10 * (bytes[off + 2] - '0') + (bytes[off + 3] - '0');
        int month = 10 * (bytes[off + 5] - '0') + (bytes[off + 6] - '0');
        int day = 10 * (bytes[off + 8] - '0') + (bytes[off + 9] - '0');

        dummyDate.setYear(year);
        dummyDate.setMonth(month);
        dummyDate.setDay(day);

        return dummyDate;
    }

    public SimpleDate getDateAttribute(int index, SimpleDate date) {
        // parse date assuming the format: "yyyy-MM-dd"
        int off = attributeOffsets[index];
        int year = 1000 * (bytes[off] - '0') + 100 * (bytes[off + 1] - '0')
                + 10 * (bytes[off + 2] - '0') + (bytes[off + 3] - '0');
        int month = 10 * (bytes[off + 5] - '0') + (bytes[off + 6] - '0');
        int day = 10 * (bytes[off + 8] - '0') + (bytes[off + 9] - '0');

        date.setYear(year);
        date.setMonth(month);
        date.setDay(day);

        return date;
    }

    /**
     * Assumes that the boolean data is represented as a single character in the
     * ascii file.
     *
     * @param index
     * @return
     */
    public boolean getBooleanAttribute(int index) {
        int off = attributeOffsets[index];

        if (bytes[off] == '1' || bytes[off] == 't')
            return true;
        else if (bytes[off] == '0' || bytes[off] == 'f')
            return false;
        else
            throw new RuntimeException("Cannot parse the boolean attribute: "
                    + bytes[off]);
    }

    @Override
    public String toString() {
        String result = String.valueOf(delimiter);
        return result;
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        dummyDate = new SimpleDate(0,0,0);
    }
}
