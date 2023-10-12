/*
 * Comparison.java
 *
 * DBMS Implementation
 */

import java.util.regex.Pattern;

/**
 * A class that represents a comparison appearing in a WHERE clause.
 */
public class Comparison extends ConditionalExpression {
    /** A comparison involving = or == */
    public static final int EQ = 0;
    
    /** A comparison involving != or <> */
    public static final int NOTEQ = 1;
    
    /** A comparison involving < */
    public static final int LT = 2;
    
    /** A comparison involving > */
    public static final int GT = 3;
    
    /** A comparison involving <= */
    public static final int LTEQ = 4;
    
    /** A comparison involving > */
    public static final int GTEQ = 5;
    
    /** A comparison involving LIKE */
    public static final int LIKE = 6;
    
    /** A comparison involving CLIKE */
    public static final int CLIKE = 7;
    
    /** A comparison involving IS NULL */
    public static final int IS_NULL = 8;
    
    /** A comparison involving IS NOT NULL */
    public static final int IS_NOT_NULL = 9;
    
    private static final int MINTYPE = 0;
    private static final int MAXTYPE = 9;
    
    private int type;
    private CompareTerm left;
    private CompareTerm right;
    private Pattern regex;
    
    /**
     * Constructs a Comparison object that represents a comparison involving
     * the specified operator and operands.
     *
     * @param  type  the type of comparison (i.e., the operator)
     * @param  left  the left operand
     * @param  right the right operand (possibly null)
     */
    public Comparison(int type, CompareTerm left, CompareTerm right) {
        if (type < MINTYPE || type > MAXTYPE)
            throw new IllegalArgumentException("invalid type: " + type);
        this.type = type;
        
        this.left = left;
        this.right = right;
        
        /* For LIKE and CLIKE, create a Pattern object for the regex. */
        if (type == LIKE || type == CLIKE) {
            /* Convert SQL regex to Java regex. */
            String expr = (String)this.right.getValue();
            expr = expr.replace("%", ".*");
            expr = expr.replace('_', '.');
            
            if (type == LIKE)
                this.regex = Pattern.compile(expr);
            else
                this.regex = Pattern.compile(expr, Pattern.CASE_INSENSITIVE);
        }
    }
    
    /**
     * Returns the left operand of the comparison represented by the called
     * object.
     *
     * @return  the left operand
     */
    public CompareTerm getLeftTerm() {
        return this.left;
    }
    
    /**
     * Returns the right operand of the comparison represented by the called
     * object.
     *
     * @return  the right operand (null if it is a IS NULL or IS NOT NULL
     *          comparison
     */
    public CompareTerm getRightTerm() {
        return this.right;
    }
    
    /**
     * Returns the type of the comparison represented by the called
     * object.
     *
     * @return  the type of the comparison -- i.e., one of the constants
     * defined in this class (e.g., Comparison.EQ for equals)
     */
    public int getType() {
        return this.type;
    }
    
    /**
     * Evaluates the comparison represented by the called object, based on
     * the current values of the operands
     *
     * @return  true if the comparison is true, and false if it is false!
     */
    public boolean isTrue() {
        Object leftVal = this.left.getValue();
        Object rightVal = (right == null ? null : this.right.getValue());
        
        // To enable comparisons between integers and reals, 
        // we turn all Integers into Doubles.
        if (leftVal != null && leftVal instanceof Integer)
            leftVal = new Double(((Integer)leftVal).doubleValue());
        if (rightVal != null && rightVal instanceof Integer)
            rightVal = new Double(((Integer)rightVal).doubleValue());
        
        // Comparisons with type mismatches are always false.
        if (leftVal != null && rightVal != null &&
            leftVal.getClass() != rightVal.getClass())
            return false;
        
        switch (left.getValType()) {
            case CompareTerm.INTEGER:
            case CompareTerm.REAL:
                return this.compareArgs((Double)leftVal, (Double)rightVal);
            case CompareTerm.STRING:
                return this.compareArgs((String)leftVal, 
                  (rightVal == null ? null : (String)rightVal));
            default:
                throw new IllegalStateException("unknown value type " + 
                  left.getValType() + " for left side of comparison");
        }
    }
    
    /*
     * A helper method that performs the actual comparison.
     */
    private <T extends Comparable<T>> boolean
      compareArgs(T leftArg, T rightArg) {
        /* 
         * Applying any standard comparison operator to a null value
         * always produces a false result.
         */
        if (this.type != IS_NULL && this.type != IS_NOT_NULL &&
            (leftArg == null || rightArg == null))
            return false;
        
        switch (this.type) {
            case EQ:
                return (leftArg.compareTo(rightArg) == 0);
            case NOTEQ:
                return (leftArg.compareTo(rightArg) != 0);
            case LT:
                return (leftArg.compareTo(rightArg) < 0);
            case GT:
                return (leftArg.compareTo(rightArg) > 0);
            case LTEQ:
                return (leftArg.compareTo(rightArg) <= 0);
            case GTEQ:
                return (leftArg.compareTo(rightArg) >= 0);
            case LIKE:
            case CLIKE:
                String leftString = leftArg.toString();
                return (this.regex.matcher(leftString).matches());
            case IS_NULL:
                return (leftArg == null);
            case IS_NOT_NULL:
                return (leftArg != null);
            default:
                throw new IllegalStateException("unknown comparison type");
        }
    }
}
