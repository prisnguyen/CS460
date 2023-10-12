/*
 * Catalog.java
 *
 * DBMS Implementation
 */

import java.io.*;
import com.sleepycat.je.*;

/**
 * A class representing the catalog of a simple relational database.  It
 * stores all per-table metadata: the name of the table, and the metadata
 * for each of the table's columns.
 *
 * To allow access to the catalog methods from all other classes, we make
 * all methods static, so that the class name can be used to invoke them.
 */
public class Catalog {
    /** The name of the catalog's underlying BDB database. */ 
    public static final String CATALOG_NAME = "catalog.db";
    
    private static Database catalogDB;
    
    /**
     * Opens the catalog so that it can be used to store and retrieve metadata.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem opening
     *         the underlying database.
     * @throws  FileNotFoundException if the Berkeley DB cannot access the
     *          catalog's database file.
     */
    public static void open() throws DatabaseException, FileNotFoundException {
        Environment env = DBMS.getEnv();
        DatabaseConfig config = new DatabaseConfig();
        config.setTransactional(true);
        config.setAllowCreate(true);
        
        // We use an auto-commit transaction to open the database, as
        // indicated by the line config.setTransactional(true) above.
        catalogDB = env.openDatabase(null, CATALOG_NAME, config);
    }
    
    /**
     * Closes the catalog and ensures all changes have been written to disk.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem closing
     *         the underlying database.
     */
    public static void close() throws DatabaseException {
        if (catalogDB != null) {
            catalogDB.close();
        }
    }
    
    /**
     * Adds an entry to the catalog for the specified table.
     *
     * @param  table  the table whose metadata we need to add
     * @return <code>OperationStatus.KEYEXIST</code> if there is already
     *         a table with the same name, and 
     *         <code>OperationStatus.SUCCESS</code> otherwise.
     * @throws DeadlockException if deadlock occurs while accessing
     *         the catalog's underlying database.
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         accessing the catalog's underlying database.
     */
    public static OperationStatus putMetadata(Table table) 
        throws DeadlockException, DatabaseException, IOException
    {
        if (table.numColumns() <= 0) {
            throw new IllegalStateException("cannot add a table to the " +
              "catalog until its column info has been initialized");
        }
        
        /* 
         * Prepare RowOutputs for both portions of the key/value pair 
         * that will be stored in the catalog for this table.
         */
        RowOutput keyBuffer = new RowOutput();
        RowOutput valueBuffer = new RowOutput();
        
        /* Write the table's name into the buffer for the key. */
        keyBuffer.writeBytes(table.getName());
        
        /* 
         * Now write the table's metadata into the buffer for the value,
         * starting with the number of columns.
         */
        valueBuffer.writeByte(table.numColumns());
        
        /* Next, write the metadata for each of the table's columns. */
        for (int i = 0; i < table.numColumns(); i++) {
            Column col = table.getColumn(i);
            
            /* Write the length of the column name, followed by the name. */
            String colName = col.getName();
            valueBuffer.writeByte(colName.length());
            valueBuffer.writeBytes(colName);
            
            /* Write the rest of the column information. */
            valueBuffer.writeByte(col.getType());
            valueBuffer.writeInt(col.getLength());
            valueBuffer.writeBoolean(col.isNotNull());
            valueBuffer.writeBoolean(col.isPrimaryKey());
        }
        
        /* Prepare the DatabaseEntry objects for the key and value. */
        DatabaseEntry key = new DatabaseEntry(keyBuffer.getBufferBytes(), 0,
                                              keyBuffer.getBufferLength());
        DatabaseEntry value = new DatabaseEntry(valueBuffer.getBufferBytes(), 0,
                                                valueBuffer.getBufferLength());

        keyBuffer.close();
        valueBuffer.close();
        
        /* 
         * Add the entry to the catalog, using putNoOverwrite() to make
         * sure there isn't already a table with the same name.
         */
        return catalogDB.putNoOverwrite(null, key, value);
    }
    
    /**
     * Reads the entry for the specified table from the catalog of
     * metadata and adds the column information to the Table object.
     *
     * @param  table  the table whose metadata we need to retrieve
     * @return <code>OperationStatus.NOTFOUND</code> if the table is not in
     *         the catalog, and <code>OperationStatus.SUCCESS</code> otherwise.
     * @throws DeadlockException if deadlock occurs while accessing
     *         the catalog's underlying database.
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         accessing the catalog's underlying database.
     */
    public static OperationStatus getMetadata(Table table)
        throws DatabaseException, DeadlockException
    {
        /* 
         * Turn the table's name into an array of bytes, 
         * and use it to create the key for the key/value pair
         * we're looking for.
         */
        byte[] nameBytes = table.getName().getBytes();
        DatabaseEntry key = new DatabaseEntry(nameBytes);
        
        /* 
         * Prepare a DatabaseEntry to receive the value portion 
         * of the key/value pair.
         */
        DatabaseEntry value = new DatabaseEntry();
        
        /* Try to get the key/value pair. */
        OperationStatus ret = catalogDB.get(null, key, value, null);
        if (ret != OperationStatus.SUCCESS) {
            return ret;
        }
        
        /* 
         * Process the table info. from the catalog entry. 
         *
         * We use a RowInput object, and read the individual pieces
         * of info. in the same order that they were written in
         * putMetadata().
         */
        RowInput buffer = new RowInput(value.getData());
        
        /* First, the number of columns. */
        int numColumns = buffer.readNextByte();
        
        /*
         * Next, the metadata for each column. 
         * We create the corresponding Column objects and add them to the
         * Table object.
         */
        for (int i = 0; i < numColumns; i++) {
            /* the column name */
            int colNameLength = buffer.readNextByte();
            String colName = buffer.readNextBytes(colNameLength);
            Column col = new Column(colName, table);
            
            /* the column info */
            col.setType(buffer.readNextByte());
            col.setLength(buffer.readNextInt());
            ColumnOptions opts = new ColumnOptions(buffer.readNextBoolean(), 
                                                   buffer.readNextBoolean());
            col.applyOpts(opts);
            col.setIndex(i);
            
            /* add the column to the table */
            table.addColumn(col);
        }
        
        return OperationStatus.SUCCESS;
    }
    
    /**
     * Removes the entry for the specified table from the catalog of metadata.
     *
     * @param  table  the table whose metadata we need to remove
     * @return <code>OperationStatus.NOTFOUND</code> if the table is not in
     *         the catalog, and <code>OperationStatus.SUCCESS</code> otherwise.
     * @throws DeadlockException if deadlock occurs while accessing
     *         the catalog's underlying database.
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         accessing the catalog's underlying database.
     */
    public static OperationStatus removeMetadata(Table table) 
        throws DeadlockException, DatabaseException
    {
        /* Prepare the key. */
        String tableName = table.getName();
        DatabaseEntry key = null;
        try {
            key = new DatabaseEntry(tableName.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            System.err.println(e);
            DBMS.abort();
        }
        
        /* Remove the entry from the catalog. */
        return catalogDB.delete(null, key);
    }
}
