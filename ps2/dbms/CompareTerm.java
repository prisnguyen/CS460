/*
 * CompareTerm.java
 *
 * DBMS Implementation
 */

/**
 * A class that represents an operand in a comparison found in the
 * WHERE clause of a SQL statement.
 */
public class CompareTerm {
    /** An integer operand */
    public static final int INTEGER = 0;
    
    /** A real (i.e., floating-point) operand */
    public static final int REAL = 1;
    
    /** A string operand */
    public static final int STRING = 2;
    
    private static final int MINTYPE = 0;
    private static final int MAXTYPE = 2;
    
    private Object value;
    private int valType;
    
    /**
     * Default constructor used when constructing a Column object
     */
    public CompareTerm() {
        // The valType will be set later using the setValType() method.
        this.valType = -1;
    }
    
    /**
     * Constructs a CompareTerm that represents an integer operand
     * with a fixed value.
     *
     * @param  i  the value of the operand
     */
    public CompareTerm(Integer i) {
        this.value = i;
        this.valType = INTEGER;
    }
    
    /**
     * Constructs a CompareTerm that represents a floating-point operand
     * with a fixed value.
     *
     * @param  d  the value of the operand
     */
    public CompareTerm(Double d) {
        this.value = d;
        this.valType = REAL;
    }
    
    /**
     * Constructs a CompareTerm that represents a String operand
     * with a fixed value.
     *
     * @param  s  the value of the operand
     */
    public CompareTerm(String s) {
        this.value = s;
        this.valType = STRING;
    }
    
    /**
     * Sets the type of the operand's value.
     *
     * @param  type  the type (one of the constants defined in this class)
     */
    public void setValType(int type) {
        if (type < MINTYPE || type > MAXTYPE)
            throw new IllegalArgumentException("invalid value type: " + type);
        this.valType = type;
    }
    
    /**
     * Gets the type of the operand's value.
     *
     * @return  the operand's type (one of the constants defined in this class)
     */
    public int getValType() {
        return this.valType;
    }
    
    /**
     * Gets the operand's value.
     *
     * @return  the operand's value
     */
    public Object getValue() {
        return this.value;
    }
    
    public String toString() {
        return this.value.toString();
    }
}
