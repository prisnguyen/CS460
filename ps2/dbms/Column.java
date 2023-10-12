/*
 * Column.java
 *
 * DBMS Implementation
 */

/**
 * A class that represents a column in a table.
 */
public class Column extends CompareTerm {
    /** A column of type INTEGER */
    public static final int INTEGER = 0;
    
    /** A column of type REAL */
    public static final int REAL = 1;
    
    /** A column of type CHAR */
    public static final int CHAR = 2;
    
    /** A column of type VARCHAR */
    public static final int VARCHAR = 3;
    
    private static final int MINTYPE = 0;
    private static final int MAXTYPE = 3;
    
    /** The print width of an INTEGER column. */
    public static final int MAX_WIDTH_INTEGER = 11;
    
    /** The print width of a REAL column. */
    public static final int MAX_WIDTH_REAL = 11;
    
    private String name;
    private Table table;
    private int type;
    private int length;
    private boolean isNotNull;
    private boolean isPrimaryKey;
    private Object updateVal;
    private boolean shouldUpdate;
    private int index;           // the index of the column in its table
    private TableIterator tableIter;
    
    /**
     * Constructs a Column object with the specified name.
     *
     * @param  name  the name of the column
     */
    public Column(String name) {
        this(name, null);
    }
    
    /**
     * Constructs a Column object with the specified name from the
     * specified table.
     *
     * @param  name  the name of the column
     * @param  t     the table to which the column belongs
     */
    public Column(String name, Table t) {
        this.name = name;
        this.table = t;
    }
    
    /**
     * Constructs a Column object that has the same initial field
     * values as the specified column.  Creating a "copy" in this way
     * allows us to have a column appear more than once in a cross
     * product, if we are crossing a table with itself.
     *
     * @param  col   the column we want to copy
     */
    public Column(Column col) {
        this.name = col.name;
        this.table = col.table;
        this.setType(col.type);
        this.length = col.length;
        this.isNotNull = col.isNotNull;
        this.isPrimaryKey = col.isPrimaryKey;
        this.index = col.index;
        this.updateVal = col.updateVal; 
        this.tableIter = col.tableIter;
    }
    
    /**
     * Determines if the column represented by the called object has
     * the same name as the column represented by the two parameters.
     * Note that we need to specify the other table, because aliases may 
     * not be reflected in the Table object associated with the Column object.
     *
     * @param   other       the other column whose name we are comparing
     *                      against
     * @param   otherTable  the other column's table
     * @return  true if the names match, and false otherwise
     */
    public boolean nameMatches(Column other, Table otherTable) {
        // First, check to see if the names of the columns match.
        if (!this.name.equals(other.name)) {
            return false;
        }
        
        // If a table wasn't specified for one or both of the columns,
        // then we assume that having the same column name indicates a
        // match.
        if (this.table == null || otherTable == null) {
            return true;
        }
        
        // Compare table names -- or aliases, if present.
        String alias1 = this.table.getRangeVar();
        String alias2 = otherTable.getRangeVar();
        String tname1 = (alias1 == null ? this.table.getName() : alias1);
        String tname2 = (alias2 == null ? otherTable.getName() : alias2);
        return (tname1.equals(tname2));
    }
    
    /**
     * Returns an adjusted value for this column, based on its type
     * and length.  This method performs any necessary type convertions
     * and length adjustments to ensure that the value is valid for this
     * column, and it will throw an <code>IllegalArgumentException</code> 
     * if the value cannot be converted (e.g., if a string is specified 
     * for an integer column).
     *
     * @param   val  the unadjusted value
     * @return  the adjusted value
     * @throws  IllegalArgumentException if the specified value cannot be
     *          converted to a valid value for this column
     */
    public Object adjustValue(Object val) {
        Object adjustedVal = val;
        String valStr;
        
        if (this.isPrimaryKey() && val == null) {
            throw new IllegalArgumentException("Null value specified " +
              "for primary key column " + this.name);
        }
        
        if (val == null) {
            if (this.isNotNull()) {
                throw new IllegalArgumentException("Null value specified " +
                  "for not-null column " + this.name);
            } else {
                return val;
            }
        }
        
        switch (this.type) {
            case Column.INTEGER:
                if (!(val instanceof Integer)) {
                    throw new IllegalArgumentException(val +
                                     ": non-integer value for integer column");
                }
                break;
            case Column.REAL:
                if (val instanceof Integer) {
                    adjustedVal = new Double(((Integer)val).doubleValue());
                } else if (!(val instanceof Double)) {
                    throw new IllegalArgumentException(val +
                      ": non-numeric value for real-valued column");
                }
                break;
            case Column.CHAR:
                valStr = val.toString();
                int valStrLen = valStr.length();
                if (valStrLen < this.length) {
                    int paddingLen = this.length - valStrLen;
                    for (int i = 0; i < paddingLen; i++) {
                        valStr += " ";
                    }
                } else if (valStrLen > this.length) {
                    valStr = valStr.substring(0, this.length);
                }
                adjustedVal = valStr;
                break;
            case Column.VARCHAR:
                valStr = val.toString();
                if (valStr.length() > this.length) {
                    valStr = valStr.substring(0, this.length);
                }
                adjustedVal = valStr;  
                break;
        }
        
        return adjustedVal;
    }
    
    /**
     * Returns the current value of this column, based on the row on which 
     * the corresponding table iterator is currently positioned.
     *
     * @return  the column's current value
     */
    public Object getValue() {
        if (this.tableIter == null) {
            return null;
        }
        return this.tableIter.getColumnVal(index);
    }
    
    /**
     * Returns the maximum print width for values from this column.
     *
     * @return  the print width
     * @throws  IllegalStateException if the column has an unknown type
     */
    public int printWidth() {
        int valWidth = 0;
        
        switch (this.getValType()) {
            case CompareTerm.INTEGER:
                valWidth = MAX_WIDTH_INTEGER;
                break;
            case CompareTerm.REAL:
                valWidth = MAX_WIDTH_REAL;
                break;
            case CompareTerm.STRING:
                valWidth = length;
                break;
            default:
                throw new IllegalStateException("unknown valType");
        }
        
        return Math.max(valWidth, this.name.length());
    }
    
    /**
     * Sets the type of this column -- which must be one of the constants
     * specified in this class (INTEGER, REAL, CHAR, or VARCHAR).
     *
     * @param   type       the column's type
     * @throws  IllegalArgumentException if an invalid type is specified
     */
    public void setType(int type) {
        if (type < MINTYPE || type > MAXTYPE) {
            throw new IllegalArgumentException("invalid type: " + type);
        }
        this.type = type;
        switch (type) {
            case INTEGER:
                this.setValType(CompareTerm.INTEGER);
                this.length = 4;
                break;
            case REAL:
                this.setValType(CompareTerm.REAL);
                this.length = 8;
                break;
            default:
                // length will be set using the setLength() method
                this.setValType(CompareTerm.STRING);
                this.length = 0;
        }
    }
    
    /**
     * Sets the length of this column.
     *
     * @param   length      the column's length
     * @throws  IllegalArgumentException if a 0 or negative length is specified
     */
    public void setLength(int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0");
        }
        this.length = length;
    }
    
    /**
     * Sets the length of this column -- i.e., the number of bytes used to
     * represent its values.  For VARCHAR columns, this should be the
     * the maximum number of bytes.  For all other columns, it should be
     * the exact number of bytes.
     *
     * @param   length      the column's length
     * @throws  IllegalArgumentException if a 0 or negative length is specified
     *          or if the argument is null
     */
    public void setLength(Integer length) {
        if (length == null) {
            throw new IllegalArgumentException("length must non-null");
        }
        this.length = length.intValue();
        if (this.length <= 0) {
            throw new IllegalArgumentException("length must be > 0");
        }
    }
    
    /**
     * Applies the specified column options to this column.
     *
     * @param   opts      the column options
     */
    public void applyOpts(ColumnOptions opts) {
        if (opts == null) {
            return;
        }
        if (opts.isNotNull()) {
            this.isNotNull = true;
        }
        if (opts.isPrimaryKey()) {
            this.isPrimaryKey = true;
        }
    }
    
    /**
     * Sets the index of this column -- i.e., its index in the
     * table to which it belongs.  The leftmost column has an index of 0.
     *
     * @param   i   the index
     * @throws  IllegalArgumentException if a negative index is specified
     */
    public void setIndex(int i) {
        if (i < 0) {
            throw new IllegalArgumentException("colNum must be >= 0");
        }
        this.index = i;
    }
    
    /**
     * Removes any update value associated with this column, which means
     * that an update command will not try to update it unless a new
     * update value is specified using <code>setUpdateVal</code>.
     */
    public void resetUpdateVal() {
        this.updateVal = null;
        this.shouldUpdate = false;
    }
    
    /**
     * Determines whether this column should be updated by an update command
     * -- i.e., if it has an up-to-date update value.
     *
     * @return  true if the column should be updated, and false otherwise
     */
    public boolean shouldUpdate() {
        return this.shouldUpdate;
    }
    
    /**
     * Sets the update value associated with this column, which means
     * that any update commands will try to update it until the update
     * value is removed using <code>resetUpdateVal</code>.
     *
     * @param   val      the value the column should be assigned
     */
    public void setUpdateVal(Object val) {
        // At the point this method is called, we don't know the column type,
        // so we can't check whether the value is valid.
        this.updateVal = val;
        this.shouldUpdate = true;
    }
    
    /**
     * Gives the column specified by the called object the same
     * metadata (type, length, etc.) as the specified other column.
     *
     * @param  other   the column whose metadata we want to copy
     * @throws IllegalArgumentException if a null argument is specified
     */
    public void useColInfo(Column other) {
        if (other == null) {
            throw new IllegalArgumentException("parameter must be non-null");
        }
        this.setType(other.type);
        this.length = other.length;
        this.isNotNull = other.isNotNull;
        this.isPrimaryKey = other.isPrimaryKey;
        this.index = other.index;
    }
    
    /**
     * Sets the table iterator used to obtain the values of this column.
     *
     * @param   iter      the table iterator
     */
    public void setTableIterator(TableIterator iter) {
        this.tableIter = iter;
    }
    
    /**
     * Returns the name of the column.
     *
     * @return  the column's name
     */
    public String getName() {
        return this.name;
    }
    
    /**
     * Returns the table to which this column belongs.
     *
     * IMPORTANT: if you invoke this method on a column from the SELECT or
     * WHERE clause with a name that isn't fully qualified (i.e., that 
     * doesn't include a table name), this method will return null.
     *
     * @return  the table
     */
    public Table getTable() {
        return this.table;
    }
    
    /**
     * Returns the type of the column
     *
     * @return  the type (one of the constants defined in this class)
     */
    public int getType() {
        return this.type;
    }
    
    /**
     * Returns the length of the column -- i.e., the number of bytes used to
     * represent its values.  For VARCHAR columns, this returns the
     * the maximum number of bytes.  For all other columns, it represents
     * the exact number of bytes.
     *
     * @return  the length
     */
    public int getLength() {
        return this.length;
    }
    
    /**
     * Returns a boolean value indicating whether this column is prevented
     * from taking on null values.
     *
     * @return  true if the column cannot take on null values, and false
     *          otherwise
     */
    public boolean isNotNull() {
        return this.isNotNull;
    }
    
    /**
     * Returns a boolean value indicating whether this column is the
     * primary key of its table
     *
     * @return  true if the column is the primary key, and false
     *          otherwise
     */
    public boolean isPrimaryKey() {
        return this.isPrimaryKey;
    }
    
    /**
     * Returns the index of the column in the table to which it
     * belongs.  The leftmost column has an index of 0.
     *
     * @return  the index
     */
    public int getIndex() {
        return this.index;
    }
    
    /**
     * Returns the update value associated with this column -- i.e., the value
     * it should be assigned by an update command.  
     *
     * This method should <i>not</i> be used to determine whether a
     * column should be updated, since this method will return
     * <code>null</code> both when there is no update value and when
     * there is an update value of <code>null</code>.  To determine if a
     * column should be updated, use the <code>shouldUpdate()</code> method.
     *
     * @return  the update value
     */
    public Object getUpdateVal() {
        return this.updateVal;
    }
    
    /**
     * Returns the table iterator used to obtain values of this column
     *
     * IMPORTANT: if you invoke this method on a Column from a Table 
     * object's array of column information, it will return null.
     * Only Column objects from the SELECT and WHERE clause have
     * associated table iterators.  These are set when the TableIterator
     * is created.
     *
     * @return  the table iterator
     */
    public TableIterator getTableIterator() {
        return this.tableIter;
    }
    
    public String toString() {
        String str = "";
        
        if (this.table != null) {
            str += (this.table + ".");
        }
        str += this.name;
        
        return str;
    }
}
