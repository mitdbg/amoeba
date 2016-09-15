package core.utils;

import core.utils.TypeUtils.SimpleDate;
import org.apache.commons.math3.ml.clustering.Clusterable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qui on 7/9/15.
 */
public class Range implements Cloneable, Clusterable {

    private Object low;
    private Object high;
    private TypeUtils.TYPE type;

    public Range(TypeUtils.TYPE type, Object low, Object high) {
        this.type = type;
        if (low == null) {
            switch (type) {
                case INT:
                    this.low = Integer.MIN_VALUE;
                    break;
                case LONG:
                    this.low = Long.MIN_VALUE;
                    break;
                case DOUBLE:
                    this.low = Double.MIN_VALUE;
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
                case DOUBLE:
                    this.high = Double.MAX_VALUE;
                    break;
                case DATE:
                    this.high = new SimpleDate(Integer.MAX_VALUE,
                            Integer.MAX_VALUE, Integer.MAX_VALUE);
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

    public TypeUtils.TYPE getType() {
        return type;
    }

    public double getLength() {
        if (low == null || high == null) {
            throw new RuntimeException("can't get length of open range");
        }
        switch (this.type) {
            case INT:
                return (Integer) this.high - (Integer) this.low;
            case LONG:
                return (Long) this.high - (Long) this.low;
            case DOUBLE:
                return (Double) this.high - (Double) this.low;
            case DATE:
                // not quite accurate, but should be fine for estimation purposes
                SimpleDate low = (SimpleDate) this.low;
                SimpleDate high = (SimpleDate) this.high;
                return 365 * (high.getYear() - low.getYear()) + 30
                        * (high.getMonth() - low.getMonth())
                        + (high.getDay() * low.getDay());
            default:
                throw new RuntimeException(this.type + " not implemented for range");
        }
    }

    // returns fraction of this range, that the intersection with the other
    // range represents
    public double intersectionFraction(Range other) {
        Range intersection = this.clone();
        intersection.intersect(other);
        return intersection.getLength() / this.getLength();
    }

    public boolean overlap(Range other) {

        //System.out.println(this.low + " " + this.high + " " + other.low + " " + other.high );

        if (this.low == null && this.high == null) return true;
        else if (this.low == null && this.high != null) {
            if (other.low == null) return true;
            else return TypeUtils.compareTo(this.high, other.low, type) >= 0;
        } else if (this.low != null && this.high == null) {
            if (other.high == null) return true;
            else return TypeUtils.compareTo(this.low, other.high, type) <= 0;
        } else { // intersection.low != null && intersection.high != null
            if (other.low == null || other.high == null) {
                return true;
            } else if (TypeUtils.compareTo(this.low, other.low, type) >= 0 && TypeUtils.compareTo(this.low, other.high, type) <= 0) {
                return true;
            } else if (TypeUtils.compareTo(this.high, other.low, type) >= 0 && TypeUtils.compareTo(this.high, other.high, type) <= 0) {
                return true;
            } else if (TypeUtils.compareTo(this.low, other.low, type) <= 0 && TypeUtils.compareTo(this.high, other.high, type) >= 0) {
                return true;
            } else
                return TypeUtils.compareTo(this.low, other.low, type) >= 0 && TypeUtils.compareTo(this.high, other.high, type) <= 0;
        }
    }


    public double jaccardSimilarity(Range other) {
        Range intersection = this.clone();
        intersection.intersect(other);
        Range union = this.clone();
        union.union(other);
        return intersection.getLength() / union.getLength();
    }

    public void intersect(Range other) {
        if (low == null) {
            low = other.low;
        }
        if (high == null) {
            high = other.high;
        }
        if ((other.low != null)
                && (TypeUtils.compareTo(other.low, low, type) == 1)) {
            low = other.low; // returns 1 if first greater, -1 if less, 0 if
            // equal.
        }
        if ((other.high != null)
                && (TypeUtils.compareTo(other.high, high, type) == -1)) {
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
        if ((other.low != null) && (low != null)
                && (TypeUtils.compareTo(other.low, low, type) == -1)) {
            low = other.low; // returns 1 if first greater, -1 if less, 0 if
            // equal.
        }
        if ((other.high != null) && (high != null)
                && (TypeUtils.compareTo(other.high, high, type) == 1)) {
            high = other.high;
        }
    }

    public void subtractLeft(Range other) {
        // todo: not general. null pointers in some cases
        Range copy = this.clone();
        copy.intersect(other);
        if (TypeUtils.compareTo(copy.low, other.low, type) == 0
                && TypeUtils.compareTo(copy.high, other.high, type) == 0) {
            low = copy.high;
        } else if (TypeUtils.compareTo(copy.low, low, type) == 0) {
            low = copy.high;
        } else if (TypeUtils.compareTo(copy.high, high, type) == 0) {
            high = copy.low;
        }
    }

    public void expand(double percentage) {
        if (low == null || high == null) {
            throw new RuntimeException("can't expand open range");
        }
        double length = getLength();
        switch (this.type) {
            case INT:
                this.low = (int) ((Integer) this.low - length * percentage);
                this.high = (int) ((Integer) this.high + length * percentage);
                break;
            case LONG:
                this.low = (long) ((Long) this.low - length * percentage);
                this.high = (long) ((Long) this.high + length * percentage);
                break;
            case DOUBLE:
                this.low = (Double) this.low - length * percentage;
                this.high = (Double) this.high + length * percentage;
                break;
            default:
                throw new RuntimeException(this.type + " not implemented for range");
        }
    }

    public List<Range> split(int numSplits) {
        double splitLength = this.getLength() / numSplits;
        List<Range> splits = new ArrayList<Range>();
        for (int i = 0; i < numSplits; i++) {
            Object splitLow;
            Object splitHigh;
            switch (type) {
                case INT:
                    splitLow = (int) (((Integer) low) + splitLength * i);
                    splitHigh = (int) (((Integer) low) + splitLength * (i + 1));
                    break;
                case LONG:
                    splitLow = (long) (((Long) low) + splitLength * i);
                    splitHigh = (long) (((Long) low) + splitLength * (i + 1));
                    break;
                case DOUBLE:
                    splitLow = ((Double) low) + splitLength * i;
                    splitHigh = ((Double) low) + splitLength * (i + 1);
                    break;
                default:
                    throw new RuntimeException("can't split non-numeric ranges");
            }
            splits.add(new Range(type, splitLow, splitHigh));
        }
        return splits;
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Range range = (Range) o;

        if (low != null ? !low.equals(range.low) : range.low != null)
            return false;
        if (high != null ? !high.equals(range.high) : range.high != null)
            return false;
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
        return "Range[" + low + ", " + high + ']';
    }

    public double[] getPoint() {
        switch (type) {
            case INT:
            case LONG:
            case DOUBLE:
                return new double[]{((Number) low).doubleValue(),
                        ((Number) high).doubleValue()};
            default:
                throw new RuntimeException("clustering of " + type.toString()
                        + " not supported");
        }
    }
}
