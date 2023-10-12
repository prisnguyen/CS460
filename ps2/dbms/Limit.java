/*
 * Limit.java
 *
 * DBMS Implementation
 */

/**
 * A class that encapsulated information from a LIMIT clause in a SELECT
 * statement.
 */
public class Limit {
    private int offset;
    private int max;
    
    /**
     * Constructs a Limit object that encapsulates the specified offset and
     * maximum number of rows for a LIMIT clause of the form<br>
     * <br>
     * <pre>LIMIT offset, max</pre>
     * <br>
     * or<br>
     * <br>
     * <pre>LIMIT max</pre>
     * <br>
     *
     * @param  offset  the offset of the first row to be displayed
     * @param  max     the maximum number of rows to be displayed
     */
    public Limit(Integer offset, Integer max) {
        if (max == null) {
            throw new IllegalArgumentException("max must be non-null");
        }
        this.offset = (offset == null ? 0 : offset.intValue());
        this.max = max.intValue();
    }
    
    /**
     * Returns the offset value of this Limit object, indicating the
     * index of the first row to be displayed.  If this value is 0,
     * the display will begin with the first row in the table.
     *
     * @return  the offset
     */
    public int getOffset() {
        return this.offset;
    }
    
    /**
     * Returns the max value of this Limit object, indicating the
     * maximum number of rows to be displayed.
     *
     * @return  the max value
     */
    public int getMax() {
        return this.max;
    }
}
