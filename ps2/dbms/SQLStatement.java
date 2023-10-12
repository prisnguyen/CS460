/*
 * SQLStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.je.*;

/**
 * An abstract class that represents a SQL statement.  An object from a
 * subclass of this class is returned by the SQL parser, and it can be
 * executed by invoking its execute() method.
 */
public abstract class SQLStatement {
    private ArrayList<Table> tables;
    private ArrayList<Column> columns;
    private ArrayList<Object> columnVals;
    private ConditionalExpression where;
    private ArrayList<Column> whereColumns;
    
    /**
     * Default constructor, used in the constructors for subclasses
     * that represent statements that don't have any associated state
     * (i.e., begin, commit, and rollback).
     */
    public SQLStatement() {
        this.tables = null;
        this.columns = null;
        this.columnVals = null;
        this.where = null;
        this.whereColumns = null;
    }
    
    /**
     * Constructor used in the constructors for subclasses that
     * represent statements that only specify a table (i.e., drop
     * table statements).
     */
    public SQLStatement(Table t) {
        this.tables = new ArrayList<Table>(1);
        this.tables.add(t);
    }
    
    /**
     * Constructor used in the constructors for subclasses that
     * represent statements that only specify a table and some column
     * information (i.e., create table statements).
     */
    public SQLStatement(Table t, ArrayList<Column> cl) {
        this.tables = new ArrayList<Table>(1);
        this.tables.add(t);
        this.columns = cl;
    }
    
    /**
     * Constructor used in the constructors for subclasses that
     * represent statements that specify table(s), column(s), and a
     * conditional expression (i.e., select, update, and delete
     * statements).
     */
    public SQLStatement(ArrayList<Table> tl, ArrayList<Column> cl,
                        ConditionalExpression w) 
    {
        this.tables = tl;
        this.columns = cl;
        this.where = w;
        
        /* Add all columns in the WHERE clause to a list. */
        this.whereColumns = new ArrayList<Column>();
        getColumnsFrom(where, whereColumns);
    }
    
    /**
     * Constructor used in the constructors for subclasses that
     * represent statements that specify a single table, one or more
     * column(s), and a conditional expression (i.e., update and delete
     * statements).
     */
    public SQLStatement(Table t, ArrayList<Column> cl,
                        ConditionalExpression w) 
    {
        this(new ArrayList<Table>(1), cl, w);
        this.tables.add(t);
    }
    
    /**
     * Constructor used in the constructors for subclasses that
     * represent statements that specify a single table, one or more
     * column(s), and values for those columns (i.e., insert statements).
     */
    public SQLStatement(Table t, ArrayList<Column> cl,
                        ArrayList<Object> vl) 
    {
        this.tables = new ArrayList<Table>(1);
        this.tables.add(t);
        this.columns = cl;
        this.columnVals = vl;
    }
    
    // helper method that finds all column objects in the specified
    // WHERE clause (exp) and adds them to the specified list
    private static void getColumnsFrom(ConditionalExpression exp,
                                       ArrayList<Column> cl)
    {
        if (exp == null) {
            return;
        } else if (exp instanceof Comparison) {
            Comparison comp = (Comparison)exp;
            CompareTerm left = comp.getLeftTerm();
            if (left instanceof Column) {
                cl.add((Column)left);
            }
            
            CompareTerm right = comp.getRightTerm();
            if (right instanceof Column) {
                cl.add((Column)right);
            }
        } else {
            // recursively traverse the left and right subtrees
            getColumnsFrom(exp.getLeft(), cl);
            getColumnsFrom(exp.getRight(), cl);
        }
    }
    
    /**
     * Executes this SQL statement.
     *
     * @throws DeadlockException if deadlock occurs while accessing
     *         an underlying database.
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         accessing an underlying database.
     */
    public abstract void execute() throws DatabaseException, DeadlockException;
    
    /**
     * Adds a table to the list of tables for this statement.
     *
     * @param  t  the table
     */
    public void addTable(Table t) {
        if (this.tables == null) {
            throw new RuntimeException("this type of statement does not " +
              "specify any tables");
        }
        this.tables.add(t);
    }
    
    /**
     * Adds a column to the list of columns for this statement.
     *
     * @param  c  the column
     */
    public void addColumn(Column c) {
        if (this.columns == null) {
            throw new RuntimeException("this type of statement does not " +
              "specify any columns");
        }
        this.columns.add(c);
    }
    
    /**
     * Adds a column value to the list of column values for this statement.
     *
     * @param  cv  the column value
     */
    public void addColumnVal(Object cv) {
        if (this.columnVals == null) {
            throw new RuntimeException("this type of statement does not " +
              "specify any column values");
        }
        this.columnVals.add(cv);
    }
    
    /**
     * Returns the table with the specified index in this statement. 
     * The first table has an index of 0.
     *
     * @return  the table
     * @throws  RuntimeException if the statement does not specify any tables
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Table getTable(int i) {
        if (this.tables == null) {
            throw new RuntimeException("this type of statement does not " +
              "specify any tables");
        }
        return this.tables.get(i);
    }
    
    /**
     * Returns the column with the specified index in this statement. 
     * The first column has an index of 0.  This method <i>cannot</i> be
     * used to obtain columns from a WHERE clause.  Use 
     * <code>getWhereColumn()</code> instead.
     *
     * @return  the column
     * @throws  RuntimeException if the statement does not specify any columns
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Column getColumn(int i) {
        if (this.columns == null) {
            throw new RuntimeException("this type of statement does not " +
              "specify any columns");
        }
        return this.columns.get(i);
    }
    
    /**
     * Returns the column value with the specified index in this statement. 
     * The first column value has an index of 0.  This method <i>cannot</i> be
     * used to obtain values from an UPDATE statement.  Instead, get the
     * column using <code>getColumn()</code> and then use its 
     * <code>getUpdateVal()</code> method.
     *
     * @return  the column value
     * @throws  RuntimeException if the statement does not specify any column
     *          values
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Object getColumnVal(int i) {
        if (this.columnVals == null) {
            throw new RuntimeException("this type of statement does not " +
              "specify any column values");
        }
        return this.columnVals.get(i);
    }
    
    /**
     * Returns the column with the specified index from the WHERE clause 
     * of this statement.  The first such column has an index of 0.
     *
     * @return  the column
     * @throws  RuntimeException if the statement does not have a WHERE clause
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Column getWhereColumn(int i) {
        if (this.whereColumns == null) {
            throw new RuntimeException("this type of statement does not " +
              "specify a WHERE clause");
        }
        return this.whereColumns.get(i);
    }
    
    public ConditionalExpression getWhere() {
        return this.where;
    }
    
    /**
     * Returns the number of tables specified in this statement.
     *
     * @return  the number of tables
     */
    public int numTables() {
        return (this.tables == null ? 0 : this.tables.size());
    }
    
    /**
     * Returns the number of columns specified in this statement.  This
     * does <i>not</i> include columns specified in the WHERE clause.
     * Use <code>numWhereColumns()</code> to get that value.
     *
     * @return  the number of columns
     */
    public int numColumns() {
        return (this.columns == null ? 0 : this.columns.size());
    }
    
    /**
     * Returns the number of columns in the WHERE clause of this statement,
     * or 0 if there is no WHERE clause.
     *
     * @return  the number of columns in the WHERE clause
     */
    public int numWhereColumns() {
        return (this.whereColumns == null ? 0 : this.whereColumns.size());
    }
    
    /**
     * Returns the number of column values specified in this statement.
     * This method <i>cannot</i> be used to obtain the number of values 
     * in an UPDATE statement.  Instead, use the <code>numColumns()</code>
     * method.
     *
     * @return  the number of column values
     */
    public int numColumnVals() {
        return (this.columnVals == null ? 0 : this.columnVals.size());
    }
    
    public String toString() {
        String str = getClass().toString();
        
        if (this.numTables() > 0 || this.numColumns() > 0 
                || this.numColumnVals() > 0) {
            str += ":";
        }
        
        if (this.numTables() > 0) {
            str += "\n    tables:";
            for (int i = 0; i < this.numTables(); i++) {
                str += ("\n      " + i + ": " + this.tables.get(i));
            }
        }
        
        if (this.numColumns() > 0) {
            str += "\n    columns:";
            for (int i = 0; i < this.numColumns(); i++) {
                str += ("\n      " + i + ": " + this.columns.get(i));
            }
        }
        
        if (this.numColumnVals() > 0) {
            str += "\n    columnVals:";
            for (int i = 0; i < this.numColumnVals(); i++) {
                str += ("\n      " + i + ": " + this.columnVals.get(i));
            }
        }
        
        if (this.numWhereColumns() > 0) {
            str += "\n  whereColumns:";
            for (int i = 0; i < this.numWhereColumns(); i++) {
                str += ("\n      " + i + ": " + this.whereColumns.get(i));
            }
        }
        
        return str;
    }
}
