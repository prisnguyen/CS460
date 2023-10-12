/*
 * ColumnOptions.java
 *
 * DBMS Implementation
 */

/**
 * A class that represents options for a column in a table -- namely,
 * whether it is not allowed to include null values, and whether it is the
 * primary key of its table.
 */
public class ColumnOptions {
    private boolean notNull;
    private boolean isPrimaryKey;
    
    /**
     * Constructs a ColumnOptions object with the specified options.
     *
     * @param  notNull     is this column not allowed to store null values?
     * @param  primaryKey  is this column the primary key?
     */
    public ColumnOptions(boolean notNull, boolean primaryKey) {
        this.notNull = notNull;
        this.isPrimaryKey = primaryKey;
    }
    
    /**
     * Returns a boolean value indicating whether the column with these
     * column options is not allowed to store null values.
     *
     * @return  true if the column not allowed to store null values, and
     *          false otherwise
     */
    public boolean isNotNull() {
        return this.notNull;
    }
    
    /**
     * Returns a boolean value indicating whether the column with these
     * column options is the primary key.
     *
     * @return  true if the column is the primary key, and false otherwise
     */
    public boolean isPrimaryKey() {
        return this.isPrimaryKey;
    }
}
