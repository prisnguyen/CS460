/*
 * CreateStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.je.*;

/**
 * A class that represents a CREATE TABLE statement.
 */
public class CreateStatement extends SQLStatement {
    /**
     * Constructs a CreateStatement object that will create the specified
     * table with the specified column information.
     *
     * @param  t  the table to be created
     * @param  colDefList  its list of column definitions
     */
    public CreateStatement(Table t, ArrayList<Column> colDefList) {
        super(t, colDefList);
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        Table table = this.getTable(0);
        
        try {
            // Add the column information to the Table object, and
            // make sure there is only one primary-key column and no
            // duplicate column names.
            boolean hasPK = false;
            for (int i = 0; i < this.numColumns(); i++) {
                Column col = this.getColumn(i);
                table.addColumn(col);
                
                if (col.isPrimaryKey()) {
                    if (hasPK) {
                        throw new Exception(table + ": may not have more " + 
                          "than one primary-key column");
                    }
                    hasPK = true;
                }
                for (int j = i+1; j < this.numColumns(); j++) {
                    if (col.getName().equals(this.getColumn(j).getName())) {
                        throw new Exception(table + ": may not have two " + 
                          "columns named " + col.getName());
                    }
                }
            }
            
            // If a primary key was not specified, use the first column.
            if (!hasPK) {
                Column first = this.getColumn(0);
                first.applyOpts(new ColumnOptions(first.isNotNull(), true));
            }
            
            // Add the metadata for the table to the catalog, printing
            // an error message if the table name is already in use.
            if (Catalog.putMetadata(table) == OperationStatus.KEYEXIST) {
                throw new Exception(table + ": a table with this name " +
                                    "already exists");
            }
            
            // Create the BDB database for the table.
            DatabaseConfig config = new DatabaseConfig();
            config.setAllowCreate(true);
            config.setTransactional(true);
            table.setDB(DBMS.getEnv().openDatabase(null, table.dbName(),
                                                   config));
            
            System.out.println("Created table " + table + ".");
        } catch (Exception e) {
            String errMsg = e.getMessage();
            if (errMsg != null) {
                System.err.println(errMsg + ".");
            }
            System.err.println("Could not create table " + table + ".");
        }
        
        // Because we could not use Table.open() above, the table is not
        // in the table cache.  Thus, we need to close its DB handle here,
        // or else it will never get closed!
        //
        // In the other commands, we will use Table.open(), so this
        // step will not be necessary.
        if (table.getDB() != null) {
            table.getDB().close();
        }
    }
}
