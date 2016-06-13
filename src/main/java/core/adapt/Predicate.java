package core.adapt;

import core.common.globals.TableInfo;
import core.utils.TypeUtils;
import core.utils.TypeUtils.TYPE;

import java.util.ArrayList;
import java.util.List;

public class Predicate {
    public int attribute;

    ;
    public TYPE type;
    public Object value;
    public PREDTYPE predtype;
    public Predicate(int attr, TYPE t, Object val, PREDTYPE predtype) {
        this.attribute = attr;
        this.type = t;
        this.value = val;
        this.predtype = predtype;
    }

    public Predicate(TableInfo tableInfo, String attr, TYPE t, Object val, PREDTYPE predtype) {
        this.attribute = tableInfo.schema.getAttributeId(attr);
        this.type = t;
        this.value = val;
        this.predtype = predtype;
    }

    public Predicate(String predString) {
        String[] tokens = predString.split(":");
        this.attribute = Integer.parseInt(tokens[0]);
        this.type = TYPE.valueOf(tokens[1]);
        this.value = TypeUtils.deserializeValue(this.type, tokens[2]);
        this.predtype = PREDTYPE.getPredType(tokens[3]);
    }

    public void normalizePredicate() {
        if (this.predtype == PREDTYPE.GEQ) {
            this.predtype = PREDTYPE.GT;
            this.value = getHelpfulCutpoint();
        } else if (this.predtype == PREDTYPE.LT) {
            this.predtype = PREDTYPE.LEQ;
            this.value = getHelpfulCutpoint();
        }
    }

    public List<Predicate> getNormalizedPredicates() {
        List<Predicate> preds = new ArrayList<Predicate>();
        if (this.type == TYPE.INT || this.type == TYPE.DATE) {
            if (this.predtype == PREDTYPE.GEQ) {
                this.predtype = PREDTYPE.GT;
                this.value = TypeUtils.deltaLess(value, type);
                preds.add(this);
            } else if (this.predtype == PREDTYPE.LT) {
                this.predtype = PREDTYPE.LEQ;
                this.value = TypeUtils.deltaLess(value, type);
                preds.add(this);
            } else if (this.predtype == PREDTYPE.EQ) {
                Predicate pred1 = new Predicate(this.attribute, this.type, this.value, PREDTYPE.LEQ);
                Predicate pred2 = new Predicate(this.attribute, this.type, TypeUtils.deltaLess(value, type), PREDTYPE.GT);
                preds.add(pred1);
                preds.add(pred2);
            } else {
                preds.add(this);
            }
        } else {
            preds.add(this);
        }
        return preds;
    }

    /**
     * Check if tuple with value for attribute is accepted (true) or rejected
     * (false) by predicate
     *
     * @param value
     * @return
     */
    public boolean isRelevant(Object value) {
        switch (this.predtype) {
            case GEQ:
                if (TypeUtils.compareTo(this.value, value, this.type) <= 0)
                    return true;
                break;
            case LEQ:
                if (TypeUtils.compareTo(this.value, value, this.type) >= 0)
                    return true;
                break;
            case GT:
                if (TypeUtils.compareTo(this.value, value, this.type) < 0)
                    return true;
                break;
            case LT:
                if (TypeUtils.compareTo(this.value, value, this.type) > 0)
                    return true;
                break;
            case EQ:
                if (TypeUtils.compareTo(this.value, value, this.type) == 0)
                    return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "" + attribute + ":" + type.toString() + ":"
                + TypeUtils.serializeValue(value, type) + ":"
                + predtype.toString();
    }

    public Object getHelpfulCutpoint() {
        switch (this.predtype) {
            case EQ:
            case GT:
            case LEQ:
                return value;
            case GEQ:
                return TypeUtils.deltaLess(value, type);

            case LT:
                if (type == TYPE.INT || type == TYPE.DATE)
                    return TypeUtils.deltaLess(value, type);
                else
                    return value;
            default:
                break;
        }
        return value;
    }

    public enum PREDTYPE {
        LEQ, GEQ, GT, LT, EQ;

        public static PREDTYPE getPredType(String predString) {
            switch (predString) {
                case "<=":
                    return LEQ;
                case ">=":
                    return GEQ;
                case ">":
                    return GT;
                case "<":
                    return LT;
                case "=":
                    return EQ;
                default:
                    throw new IllegalArgumentException("predicate string " + predString + " not a valid pred");
            }
        }

        @Override
        public String toString() {
            switch (this) {
                case LEQ:
                    return "<=";
                case GEQ:
                    return ">=";
                case GT:
                    return ">";
                case LT:
                    return "<";
                case EQ:
                    return "=";
                default:
                    return "=";
            }
        }
    }
}
