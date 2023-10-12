/*
 * DeleteStatement.java
 *
 * DBMS Implementation
 */

import com.sleepycat.je.*;

/**
 * A class that represents a DELETE statement.
 */
public class DeleteStatement extends SQLStatement {
    /** 
     * Constructs a DeleteStatement object involving the specified
     * table and conditional expression.  If no conditional expression 
     * is specified, all rows will be deleted.
     *
     * @param  t  the table in which the deletes should occur
     * @param  where  the conditional expression from the WHERE clause
     */
    public DeleteStatement(Table t, ConditionalExpression where) {
        super(t, null, where);
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        /* not implemented for this assignment */
    }
}
