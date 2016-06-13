package core.utils;

import com.google.common.primitives.Ints;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;

/**
 * A set of binary utilities to convert to and from different data types to
 * binary representation
 */
public class BinaryUtils {

    /**
     * The data format parser
     */
    public final static DateFormat dateParser = new SimpleDateFormat(
            "yyyy-MM-dd");

    /**
     * Convert four bytes from a byte array to integer value.
     *
     * @param a   The byte Array
     * @param off The offset from which to get the integer bytes
     * @return Integer value
     */
    public static short getShort(byte[] a, int off) {
        short shortValue = 0;
        shortValue += (a[off] & 0x00FF) << 8;
        shortValue += (a[off + 1] & 0x00FF);
        return shortValue;
    }

    /**
     * Convert four bytes from a byte array to integer value.
     *
     * @param a   The byte Array
     * @param off The offset from which to get the integer bytes
     * @return Integer value
     */
    public static int getInt(byte[] a, int off) {
        int intValue = 0;
        intValue += (a[off] & 0x000000FF) << 24;
        intValue += (a[off + 1] & 0x000000FF) << 16;
        intValue += (a[off + 2] & 0x000000FF) << 8;
        intValue += (a[off + 3] & 0x000000FF);
        return intValue;
    }

    /**
     * Convert eight bytes from a byte array to long value.
     *
     * @param a   The byte array
     * @param off The offset from which to get the long bytes
     * @return Long value
     */
    public static long getLong(byte[] a, int off) {
        long longValue = 0;
        longValue += (long) (a[off] & 0x00000000000000FF) << 56;
        longValue += (long) (a[off + 1] & 0x00000000000000FF) << 48;
        longValue += (long) (a[off + 2] & 0x00000000000000FF) << 40;
        longValue += (long) (a[off + 3] & 0x00000000000000FF) << 32;
        longValue += (long) (a[off + 4] & 0x00000000000000FF) << 24;
        longValue += (long) (a[off + 5] & 0x00000000000000FF) << 16;
        longValue += (long) (a[off + 6] & 0x00000000000000FF) << 8;
        longValue += (long) (a[off + 7] & 0x00000000000000FF);
        return longValue;
    }

    /**
     * Convert eight bytes from a byte array to float value.
     *
     * @param a   The byte array
     * @param off The offset from which to get the float value
     * @return Float value
     */
    public static float getFloat(byte[] a, int off) {
        int accum = 0;
        accum |= (a[off] & 0xff) << 24;
        accum |= (a[off + 1] & 0xff) << 16;
        accum |= (a[off + 2] & 0xff) << 8;
        accum |= (a[off + 3] & 0xff);
        // int i = 0;
        // for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
        // accum |= ((long) (a[off + i] & 0xff)) << shiftBy;
        // i++;
        // }
        return Float.intBitsToFloat(accum);
    }

    /**
     * Capture a portion of byte array as string value.
     *
     * @param a       The byte array
     * @param off     The offset from which to capture the string value
     * @param strSize The size of the string value
     * @return String value
     */
    public static String getString(byte[] a, int off, int strSize) {
        return new String(a, off, strSize);
    }

    /**
     * Capture a portion of byte array as date value.
     *
     * @param a        The byte array
     * @param off      The offset from which to capture the string value
     * @param dateSize The size of the date value
     * @return Date value
     */
    public static Date getDate(byte[] a, int off, int dateSize) {
        try {
            return dateParser.parse(new String(a, off, dateSize));
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static int getInt(byte[] a, int start, int end) {
        int val = 0;
        for (int i = start; i <= end; i++)
            val += (a[i] - 48) * Math.pow(10, (end - i));
        return val;
    }

    public static long getLong(byte[] a, int start, int end) {
        long val = 0;
        for (int i = start; i <= end; i++)
            val += (a[i] - 48) * Math.pow(10, (end - i));
        return val;
    }

    public static float getFloat(byte[] a, int start, int end) {
        float val = 0;
        int decimalIdx = end;
        for (int i = start; i <= end; i++) {
            if (a[i] == '.')
                decimalIdx = i;
            else if (i < decimalIdx)
                val += (a[i] - 48) * Math.pow(10, (end - i - 1));
            else
                val += (a[i] - 48) * Math.pow(10, (end - i));
        }
        val /= Math.pow(10, (end - decimalIdx));
        return val;
    }

    public static byte[] getString(byte[] a, int start, int end, int size) {
        byte[] val = new byte[size];
        System.arraycopy(a, start, val, 0, end - start + 1);
        for (int i = end - start + 1; i < val.length; i++)
            val[i] = ' ';
        return val;
    }

    /**
     * Append an short value to a byte array.
     *
     * @param a      The byte array
     * @param offset The offset in the byte array to append at
     * @param val    The short value to be appended
     */
    public static void append(byte[] a, int offset, short val) {
        a[offset] = (byte) (val >> 8);
        a[offset + 1] = (byte) (val >> 0);
    }

    /**
     * Append an integer value to a byte array.
     *
     * @param a      The byte array
     * @param offset The offset in the byte array to append at
     * @param val    The integer value to be appended
     */
    public static void append(byte[] a, int offset, int val) {
        a[offset] = (byte) (val >> 24);
        a[offset + 1] = (byte) (val >> 16);
        a[offset + 2] = (byte) (val >> 8);
        a[offset + 3] = (byte) (val >> 0);
    }

    /**
     * Append an float value to a byte array.
     *
     * @param a      The byte array
     * @param offset The offset in the byte array to append at
     * @param val    The float value to be appended
     */
    public static void append(byte[] a, int offset, float val) {
        int n = Float.floatToIntBits(val);
        a[offset] = (byte) (n >> 24);
        a[offset + 1] = (byte) (n >> 16);
        a[offset + 2] = (byte) (n >> 8);
        a[offset + 3] = (byte) (n >> 0);
    }

    /**
     * Append an long value to a byte array.
     *
     * @param a      The byte array
     * @param offset The offset in the byte array to append at
     * @param val    The long value to be appended
     */
    public static void append(byte[] a, int offset, long val) {
        a[offset] = (byte) (val >> 56);
        a[offset + 1] = (byte) (val >> 48);
        a[offset + 2] = (byte) (val >> 40);
        a[offset + 3] = (byte) (val >> 32);
        a[offset + 4] = (byte) (val >> 24);
        a[offset + 5] = (byte) (val >> 16);
        a[offset + 6] = (byte) (val >> 8);
        a[offset + 7] = (byte) (val >> 0);
    }

    /**
     * Append an string value to a byte array.
     *
     * @param a      The byte array
     * @param offset The offset in the byte array to append at
     * @param val    The string value to be appended
     */
    public static void append(byte[] a, int offset, String val) {
        byte[] tmp = val.getBytes();
        System.arraycopy(tmp, 0, a, offset, tmp.length);
    }

    /**
     * Append an date value to a byte array.
     *
     * @param a      The byte array
     * @param offset The offset in the byte array to append at
     * @param val    The date value to be appended
     */
    public static void append(byte[] a, int offset, Date val) {
        byte[] tmp = dateParser.format(val).getBytes();
        System.arraycopy(tmp, 0, a, offset, tmp.length);
    }

    /**
     * Append a byte array to a given byte array
     *
     * @param a      The byte array
     * @param offset The offset in the byte array to append at
     * @param val    The byte array to be appended
     */
    public static void append(byte[] a, int offset, byte[] val) {
        System.arraycopy(val, 0, a, offset, val.length);
    }

    /**
     * Convert an short value to its byte array representation.
     *
     * @param val Short value
     * @return Byte array representation
     */
    public static byte[] toBytes(short val) {
        byte[] a = new byte[2];
        a[0] = (byte) (val >> 8);
        a[1] = (byte) (val >> 0);
        return a;
    }

    /**
     * Convert an integer value to its byte array representation.
     *
     * @param val Integer value
     * @return Byte array representation
     */
    public static byte[] toBytes(int val) {
        byte[] a = new byte[4];
        a[0] = (byte) (val >> 24);
        a[1] = (byte) (val >> 16);
        a[2] = (byte) (val >> 8);
        a[3] = (byte) (val >> 0);
        return a;
    }

    /**
     * Convert an long value to its byte array representation.
     *
     * @param val Long value
     * @return Byte array representation
     */
    public static byte[] toBytes(long val) {
        byte[] a = new byte[8];
        a[0] = (byte) (val >> 56);
        a[1] = (byte) (val >> 48);
        a[2] = (byte) (val >> 40);
        a[3] = (byte) (val >> 32);
        a[4] = (byte) (val >> 24);
        a[5] = (byte) (val >> 16);
        a[6] = (byte) (val >> 8);
        a[7] = (byte) (val >> 0);

        return a;
    }

    /**
     * Convert an float value to its byte array representation.
     *
     * @param val Float value
     * @return Byte array representation
     */
    public static byte[] toBytes(float val) {
        byte[] a = new byte[4];
        int n = Float.floatToIntBits(val);
        a[0] = (byte) (n >> 24);
        a[1] = (byte) (n >> 16);
        a[2] = (byte) (n >> 8);
        a[3] = (byte) (n >> 0);
        return a;
    }

    /**
     * Convert an string value to its byte array representation.
     *
     * @param val String value
     * @return Byte array representation
     */
    public static byte[] toBytes(String val) {
        return val.getBytes();
    }

    /**
     * Convert an date value to its byte array representation.
     *
     * @param val Date value
     * @return Byte array representation
     */
    public static byte[] toBytes(Date val) {
        return dateParser.format(val).getBytes();
    }

    /**
     * Compute and return the maximum position of non-zero bit.
     *
     * @param number
     * @return maximum non-zero bit position
     */
    public static int nonzeroBits(int num) {
        int c = 0;
        while (num != 0) {
            c++;
            num = num >> 1;
        }
        return c;
    }

    /**
     * Pack an integer value into a bit set.
     *
     * @param bs       The bit set to pack the integer value into
     * @param offset   The offset in the bit set for the integer value
     * @param data     The integer value to be packed
     * @param noOfBits Number of bits used for packing
     * @return Number of bits used for packing
     */
    public static int packInt(BitSet bs, int offset, int data, int noOfBits) {
        for (int i = 1; i <= noOfBits; i++) {
            if ((data & (1 << (noOfBits - i))) > 0)
                bs.set(offset + i - 1, true);
            else
                bs.set(offset + i - 1, false);
        }
        return offset + noOfBits;
    }

    /**
     * Unpack an integer value from a bit set.
     *
     * @param bs       The bit set to unpack the integer value from
     * @param offset   The offset in the bit set for the integer value
     * @param noOfBits Number of bits used for unpacking
     * @return The unpacked integer value
     */
    public static int unpackInt(BitSet bs, int offset, int noOfBits) {
        int val = 0;
        for (int i = 0; i < noOfBits; i++) {
            if (bs.get(offset + i))
                val |= 1 << (noOfBits - i - 1);
        }
        return val;
    }

    /**
     * Convert a bit set into a byte array. Note that the bit set may actually
     * be using non-integral number of bytes, but the conversion aligns it to
     * byte boundary.
     *
     * @param bs     The bit set to be converted into byte array
     * @param length The number of bits used in the bit set
     * @return The byte array derived from the bit set
     */
    public static byte[] toByteArray(BitSet bs, int length) {
        byte[] bytes = new byte[(int) Math.ceil((double) length / 8)];
        for (int i = 0; i < length; i++) {
            if (bs.get(i))
                bytes[i / 8] |= 1 << (7 - (i % 8));
        }
        return bytes;
    }

    /**
     * Convert a byte array into a bit set.
     *
     * @param bytes The byte array to be converted into a bit set
     * @return The equivalent bit set
     */
    public static BitSet toBitSet(byte[] bytes) {
        BitSet bs = new BitSet();
        for (int i = 0; i < bytes.length * 8; i++) {
            if ((bytes[i / 8] & (1 << (7 - (i % 8)))) > 0)
                bs.set(i, true);
            else
                bs.set(i, false);
        }
        return bs;
    }

    /**
     * Extract a sub byte array from a given byte array.
     *
     * @param a      The given byte array
     * @param offset The offset in the array to extract sub array from
     * @param len    Number of bytes to extract for the sub-array
     * @return The sub byte array
     */
    public static byte[] getBytes(byte[] a, int offset, int len) {
        byte[] bytes = new byte[len];
        System.arraycopy(a, offset, bytes, 0, bytes.length);
        return bytes;
    }

    public static byte[] getAndPadBytes(byte[] a, int offset, int len, int pad) {
        byte[] bytes = new byte[len + pad];
        System.arraycopy(a, offset, bytes, 0, len);
        for (int i = len; i < len + pad; i++)
            a[i] = 32;
        return bytes;
    }

    /**
     * Concatenate two byte arrays.
     *
     * @param b1 First byte array
     * @param b2 Second byte array
     * @return Concatenated byte array
     */
    public static byte[] concatenate(byte[] b1, byte[] b2) {
        byte[] a = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, a, 0, b1.length);
        System.arraycopy(b2, 0, a, b1.length, b2.length);
        return a;
    }

    public static byte[] resize(byte[] a, int newSize) {
        if (a.length == newSize)
            return a;

        byte[] b = new byte[newSize];
        if (a.length < newSize)
            System.arraycopy(a, 0, b, 0, a.length);
        else
            System.arraycopy(a, 0, b, 0, newSize);
        return b;
    }

    public static byte[][] resize(byte[][] a, int newSize) {
        if (a.length == newSize)
            return a;

        byte[][] b = new byte[newSize][];
        if (a.length < newSize)
            System.arraycopy(a, 0, b, 0, a.length);
        else
            System.arraycopy(a, 0, b, 0, newSize);
        return b;
    }

    public static byte[][] resizeToEqual(byte[][] a) {
        int newSize = 0;
        for (int i = 0; i < a.length; i++)
            if (newSize < a[i].length)
                newSize = a[i].length;

        byte[][] b = new byte[a.length][];
        for (int i = 0; i < b.length; i++) {
            if (a[i].length < newSize) {
                b[i] = new byte[newSize];
                System.arraycopy(a[i], 0, b[i], 0, a[i].length);
            } else
                b[i] = a[i];
        }
        return b;
    }

    public static int compareInt(byte[] bytes1, int offset1, byte[] bytes2,
                                 int offset2) {
        if ((bytes1[offset1] & 0x000000FF) > (bytes2[offset2] & 0x000000FF))
            return 1;
        else if ((bytes1[offset1] & 0x000000FF) < (bytes2[offset2] & 0x000000FF))
            return -1;
        if ((bytes1[offset1 + 1] & 0x000000FF) > (bytes2[offset2 + 1] & 0x000000FF))
            return 1;
        else if ((bytes1[offset1 + 1] & 0x000000FF) < (bytes2[offset2 + 1] & 0x000000FF))
            return -1;
        if ((bytes1[offset1 + 2] & 0x000000FF) > (bytes2[offset2 + 2] & 0x000000FF))
            return 1;
        else if ((bytes1[offset1 + 2] & 0x000000FF) < (bytes2[offset2 + 2] & 0x000000FF))
            return -1;
        if ((bytes1[offset1 + 3] & 0x000000FF) > (bytes2[offset2 + 3] & 0x000000FF))
            return 1;
        else if ((bytes1[offset1 + 3] & 0x000000FF) < (bytes2[offset2 + 3] & 0x000000FF))
            return -1;
        else
            return 0;
    }

    public static int compareDate(byte[] bytes1, int offset1, byte[] bytes2,
                                  int offset2) {
        // compare dates of the following format: "yyyy-MM-dd"

        int cmp;
        cmp = Ints.compare(getInt(bytes1, offset1, offset1 + 3),
                getInt(bytes2, offset2, offset2 + 3));
        if (cmp != 0)
            return cmp;

        cmp = Ints.compare(getInt(bytes1, offset1 + 5, offset1 + 6),
                getInt(bytes2, offset2 + 5, offset2 + 6));
        if (cmp != 0)
            return cmp;

        return Ints.compare(getInt(bytes1, offset1 + 8, offset1 + 9),
                getInt(bytes2, offset2 + 8, offset2 + 9));
    }

    public static void main(String[] args) {
        byte[][] a = new byte[][]{{1, 2, 3}, {4, 5}, {6}, {}};
        byte[][] b = resizeToEqual(a);
        for (int i = 0; i < b.length; i++) {
            for (int j = 0; j < b[i].length; j++)
                System.out.print(b[i][j] + " ");
            System.out.println();
        }

        // float val = (float) 0.2;
        // byte[] valBytes = BinaryUtils.toBytes(val);
        // float valAgain = BinaryUtils.getFloat(valBytes, 0);
        //
        // System.out.println(Arrays.toString(valBytes));
        // System.out.println(valAgain);

        // byte[] bytes1 = BinaryUtils.toBytes(511);
        // byte[] bytes2 = BinaryUtils.toBytes(256);
        // System.out.println(BinaryUtils.getInt(bytes1, 0));
        // System.out.println(BinaryUtils.getInt(bytes2, 0));
        // System.out.println(Arrays.toString(bytes1));
        // System.out.println(Arrays.toString(bytes2));
        // System.out.println(BinaryUtils.compareInt(bytes1, 0, bytes2, 0));

        // String str = "test123";
        // byte[] a = BinaryUtils.getAndPadBytes(str.getBytes(), 0, 4, 3);
        // System.out.println(a.length);
        // System.out.println(new String(a)+"123");
    }
}
