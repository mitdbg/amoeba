package core.utils;

import java.util.Calendar;
import java.util.Comparator;
import java.util.GregorianCalendar;

public class TypeUtils {
    public static int compareTo(Object x, Object y, TYPE t) {
        switch (t) {
            case INT:
                return ((Integer) x).compareTo((Integer) y);
            case DOUBLE:
                return ((Double) x).compareTo((Double) y);
            case LONG:
                return ((Long) x).compareTo((Long) y);
            case DATE:
                return ((SimpleDate) x).compareTo((SimpleDate) y);
            case STRING:
                return ((String) x).compareTo((String) y);
            default:
                System.err.println("Unknown TYPE in compareTo");
                return 0;
        }
    }

    ;

    public static Comparator<Object> getComparatorForType(TYPE type) {
        switch (type) {
            case INT:
                return new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Integer) o1).compareTo((Integer) o2);
                    }
                };
            case LONG:
                return new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Long) o1).compareTo((Long) o2);
                    }
                };
            case DOUBLE:
                return new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Double) o1).compareTo((Double) o2);
                    }
                };
            case DATE:
                return new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((SimpleDate) o1).compareTo((SimpleDate) o2);
                    }
                };
            case STRING:
                return new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((String) o1).compareTo((String) o2);
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

    public static String serializeValue(Object value, TYPE t) {
        return value.toString();
    }

    public static Object deserializeValue(TYPE t, String token) {
        switch (t) {
            case INT:
                return Integer.parseInt(token);
            case LONG:
                return Long.parseLong(token);
            case DOUBLE:
                return Double.parseDouble(token);
            case DATE:
                byte[] bytes = token.getBytes();
                int off = 0;
                int year = 1000 * (bytes[off] - '0') + 100 * (bytes[off + 1] - '0')
                        + 10 * (bytes[off + 2] - '0') + (bytes[off + 3] - '0');
                int month = 10 * (bytes[off + 5] - '0') + (bytes[off + 6] - '0');
                int day = 10 * (bytes[off + 8] - '0') + (bytes[off + 9] - '0');

                return new SimpleDate(year, month, day);
            case BOOLEAN:
                return Boolean.parseBoolean(token);
            case STRING:
                return token;
            case VARCHAR:
                return token;
            default:
                return token;
        }
    }

    public static Object deltaLess(Object value, TYPE t) {
        switch (t) {
            case INT:
                return (Integer) value - 1;
            case LONG:
                return (Long) value - 1;
            case DOUBLE:
                return (Double) value - 0.001;
            case DATE:
                SimpleDate d = (SimpleDate) value;
                return d.oneDayLess();
            case BOOLEAN:
                return false;
            case STRING:
                System.err.println("ERR: Called deltaLess on string");
                return value;
            case VARCHAR:
                System.err.println("ERR: Called deltaLess on string");
                return value;
            default:
                System.err.println("ERR: Called deltaLess on unknown type");
                return value;
        }
    }

    public enum TYPE {
        BOOLEAN, INT, LONG, DOUBLE, STRING, DATE, VARCHAR
    }

    public static class SimpleDate implements Comparable<SimpleDate> {
        public static int[] daysPerMonth = new int[]{31, 28, 31, 30, 31, 30,
                31, 31, 30, 31, 30, 31};
        private int year, month, day;

        public SimpleDate(int year, int month, int day) {
            this.year = year;
            this.month = month;
            this.day = day;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getMonth() {
            return month;
        }

        public void setMonth(int month) {
            this.month = month;
        }

        public int getDay() {
            return day;
        }

        public void setDay(int day) {
            this.day = day;
        }

        // Though expensive to create the calendar and find out, it works.
        // Infrequent operation, not worth optimizing.
        public SimpleDate oneDayLess() {
            // Month in the gregorian calendar class is 0 based.
            Calendar c = new GregorianCalendar(year, month - 1, day);
            c.add(Calendar.DAY_OF_MONTH, -1);
            return new SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1,
                    c.get(Calendar.DAY_OF_MONTH));
        }

        @Override
        public int compareTo(SimpleDate d) {
            if (d.getYear() < year
                    || (d.getYear() == year && d.getMonth() < month)
                    || (d.getYear() == year && d.getMonth() == month && d
                    .getDay() < day))
                return 1;
            else if (d.getYear() == year && d.getMonth() == month
                    && d.getDay() == day)
                return 0;
            else
                return -1;
        }

        @Override
        public boolean equals(Object obj) {
            SimpleDate d = (SimpleDate) obj;
            return (d.getYear() == year && d.getMonth() == month && d.getDay() == day);
        }

        @Override
        public String toString() {
            String ret = "" + year + "-";
            ret += (month < 10 ? "0" + month : month) + "-";
            ret += (day < 10 ? "0" + day : day);
            return ret;
        }
    }
}
