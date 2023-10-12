/*
 * InsertStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.je.*;

/**
 * A class that represents an INSERT statement.
 */
public class InsertStatement extends SQLStatement {
    /** 
     * Constructs an InsertStatement object involving the specified table,
     * list of columns (if any), and list of values.  The columns and 
     * their associated values must be specified in the same order.
     * If no list of columns is specified, we will assume that values are 
     * being specified for all columns.
     *
     * @param  t  the table in which the values should be inserted
     * @param  colList  the list of columns for which values are specified
     * @param  valList  the list of values to be inserted
     */
    public InsertStatement(Table t, ArrayList<Column> colList,
                           ArrayList<Object> valList) {
        super(t, colList, valList);
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        try {
            Table table = this.getTable(0);
            if (table.open() != OperationStatus.SUCCESS) {
                throw new Exception();  // error msg was printed in open()
            }
            
            // Preliminary error checking.
            if (this.numColumns() != 0) {
                throw new Exception("INSERT commands with column names " +
                                    "are not supported");
            }
            if (this.numColumnVals() != table.numColumns()) {
                throw new Exception("Must specify a value for each column");
            }
            
            // Make any necessary adjustments (type conversions, 
            // truncations, etc.) to the values to be inserted.
            // This will throw an exception if a value is invalid.
            Object[] adjustedValues = new Object[table.numColumns()];
            for (int i = 0; i < table.numColumns(); i++) {
                Column col = table.getColumn(i);
                adjustedValues[i] = col.adjustValue(this.getColumnVal(i));
            }
            
            // Create an InsertRow object for the row to be inserted,
            // and use that object to marshall the row.
            InsertRow row = new InsertRow(table, adjustedValues);
            row.marshall();
            if (DBMS.DEBUG) {
                System.out.println(row);
            }
            
            /* 
             * PS 2: Add code below to perform the actual insertion, 
             * and to print the appropriate message after it has occurred.
             */
            
            
        } catch (Exception e) {
            String errMsg = e.getMessage();
            if (errMsg != null) {
                System.err.println(errMsg + ".");
            }
            System.err.println("Could not insert row.");
        }
    }
}
