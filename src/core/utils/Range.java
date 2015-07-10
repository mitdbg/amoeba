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

    public Object getLow() {
        return low;
    }

    public Object getHigh() {
        return high;
    }

    public SchemaUtils.TYPE getType() {
        return type;
    }

    public double getLength() {
        if (low == null || high == null) {
            throw new RuntimeException("can't get length of open range");
        }
        switch (this.type) {
            case INT:
                return (double) ((Integer)this.high - (Integer)this.low);
            case LONG:
                return (double) ((Long)this.high - (Long)this.low);
            case FLOAT:
                return (double) ((Float)this.high - (Float)this.low);
            case DATE:
                // not quite accurate, but should be fine for estimation purposes
                SimpleDate low = (SimpleDate)this.low;
                SimpleDate high = (SimpleDate)this.high;
                return (double) (365*(high.getYear()-low.getYear()) + 30*(high.getMonth()-low.getMonth()) + (high.getDay()*low.getDay()));
            default:
                throw new RuntimeException(this.type+" not implemented for range");
        }
    }

    // returns fraction of this range, that the intersection with the other range represents
    public double intersectionFraction(Range other) {
        Range intersection = this.clone();
        intersection.intersect(other);
        return intersection.getLength() / this.getLength();
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

    public void union(Range other) {
        if (other.low == null) {
            low = other.low;
        }
        if (other.high == null) {
            high = other.high;
        }
        if ((other.low != null) && (low != null) && (TypeUtils.compareTo(other.low, low, type) == -1)) {
            low = other.low; // returns 1 if first greater, -1 if less, 0 if equal.
        }
        if ((other.high != null) && (high != null) && (TypeUtils.compareTo(other.high, high, type) == 1)) {
            high = other.high;
        }
    }

    public void expand(double percentage) {
        if (low == null || high == null) {
            throw new RuntimeException("can't expand open range");
        }
        double length = getLength();
        switch (this.type) {
            case INT:
                this.low = (int)((Integer)this.low - length * percentage);
                this.high = (int)((Integer)this.high + length * percentage);
                break;
            case LONG:
                this.low = (long)((Long)this.low - length * percentage);
                this.high = (long)((Long)this.high + length * percentage);
                break;
            case FLOAT:
                this.low = (float)((Float)this.low - length * percentage);
                this.high = (float)((Float)this.high + length * percentage);
                break;
            default:
                throw new RuntimeException(this.type+" not implemented for range");
        }
    }

    public boolean contains(Range other) {
        Range me = this.clone();
        me.intersect(other);
        return me.equals(other);
    }

    @Override
    public Range clone() {
        return new Range(type, low, high);
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
