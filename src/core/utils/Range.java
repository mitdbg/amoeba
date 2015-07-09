package core.utils;

import core.utils.RangeUtils.SimpleDateRange.SimpleDate;

/**
 * Created by qui on 7/9/15.
 */
public class Range implements Cloneable {

    private Object low;
    private Object high;
    private SchemaUtils.TYPE type;

    public Range(SchemaUtils.TYPE type, Object low, Object high) {
        this.type = type;
        if (low == null) {
            switch (type) {
                case INT:
                    this.low = Integer.MIN_VALUE;
                    break;
                case LONG:
                    this.low = Long.MIN_VALUE;
                    break;
                case FLOAT:
                    this.low = Float.MIN_VALUE;
                    break;
                case DATE:
                    this.low = new SimpleDate(-1, -1, -1);
                    break;
                default:
                    this.low = null;
            }
        } else {
            this.low = low;
        }
        if (high == null) {
            switch (type) {
                case INT:
                    this.high = Integer.MAX_VALUE;
                    break;
                case LONG:
                    this.high = Long.MAX_VALUE;
                    break;
                case FLOAT:
                    this.high = Float.MAX_VALUE;
                    break;
                case DATE:
                    this.high = new SimpleDate(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
                    break;
                default:
                    this.high = null;
            }
        } else {
            this.high = high;
        }
    }

    @Override
    public Range clone() {
        return new Range(type, low, high);
    }

    public void intersect(Range other) {
        if (low == null) {
            low = other.low;
        }
        if (high == null) {
            high = other.high;
        }
        if ((other.low != null) && (TypeUtils.compareTo(other.low, low, type) == 1)) {
            low = other.low; // returns 1 if first greater, -1 if less, 0 if equal.
        }
        if ((other.high != null) && (TypeUtils.compareTo(other.high, high, type) == -1)) {
            high = other.high;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Range range = (Range) o;

        if (low != null ? !low.equals(range.low) : range.low != null) return false;
        if (high != null ? !high.equals(range.high) : range.high != null) return false;
        return type == range.type;
    }

    @Override
    public int hashCode() {
        int result = low != null ? low.hashCode() : 0;
        result = 31 * result + (high != null ? high.hashCode() : 0);
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Range[" +
                low +
                ", " + high +
                ']';
    }
}
