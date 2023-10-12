/*
 * BeginStatement.java
 *
 * DBMS Implementation
 */

import com.sleepycat.je.*;

/**
 * A class that represents a BEGIN WORK statement, which begins a
 * transaction.
 */
public class BeginStatement extends SQLStatement {
    /** Constructs a BeginStatement object */
    public BeginStatement() {
        super();
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        /* not implemented for this assignment */
    }
}
