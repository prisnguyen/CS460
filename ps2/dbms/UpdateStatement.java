/*
 * UpdateStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.je.*;

/**
 * A class that represents an UPDATE statement.
 */
public class UpdateStatement extends SQLStatement {
    /** 
     * Constructs an UpdateStatement object involving the specified
     * table, list of columns, and conditional expression.  If no
     * conditional expression is specified, the update will be applied to
     * all rows in the table.  <i>Note</i>: the update values should have
     * been set within the Column objects in the list of columns before
     * this constructor is invoked.
     *
     * @param  t  the table in which the updates should occur
     * @param  colList  the list of columns for which updates are specified
     * @param  where  the conditional expression from the WHERE clause
     */
    public UpdateStatement(Table t, ArrayList<Column> colList,
                           ConditionalExpression where) {
        super(t, colList, where);
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        /* not implemented for this assignment */
    }
}
