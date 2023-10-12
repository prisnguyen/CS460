/*
 * RollbackStatement.java
 *
 * DBMS Implementation
 */

import com.sleepycat.je.*;

/**
 * A class that represents a ROLLBACK statement in SQL, which rolls back
 * the current transaction.
 */
public class RollbackStatement extends SQLStatement {
    public RollbackStatement() {
        super();
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        /* not implemented for this assignment */
    }
}
