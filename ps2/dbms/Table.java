/*
 * Table.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.je.*;

/**
 * A class that represents a table in a relational database.
 */
public class Table {
    /* A cache of Table objects for all open tables. */
    private static Hashtable<String, Table> tableCache;
    private static final int TABLE_CACHE_INIT_SIZE = 25;
    
    private String name;
    private String rangeVar;
    private ArrayList<Column> columns;
    private Database db;
    
    /**
     * Constructs a Table object with the specified name.
     *
     * @param  name  the name of the table
     */
    public Table(String name) {
        this.name = name;
    }
    
    /**
     * Opens the table so that it can be accessed by SQL commands.
     * Opens the Database handle for the underlying BDB database, and
     * fills in the Table object with the metadata stored in the
     * catalog for this table.
     *
     * IMPORTANT: this should only be used to open tables that have
     * already been created.  New tables should be opened in the code that
     * implements the CREATE TABLE command.
     *
     * @return <code>OperationStatus.NOTFOUND</code> if the table does 
     *         not exist, and <code>OperationStatus.SUCCESS</code> otherwise.
     * @throws DatabaseException if Berkeley DB encounters a problem opening
     *         the underlying database.
     */
    public OperationStatus open() throws DatabaseException {
        /* 
         * If there is a table with this name in the table cache,
         * use its database handle and copy its column information.
         */
        Table cachedTable = tableCache.get(this.name);
        if (cachedTable != null) {
            this.db = cachedTable.db;
            
            /* 
             * Create a columns array for this Table object from the
             * columns array of the cached Table object.  
             * We create a new array so that the Column objects will
             * have access to information associated with this Table
             * object (e.g., its alias, if any).
             */
            this.columns = new ArrayList<Column>();
            for (int i = 0; i < cachedTable.columns.size(); i++) {
                Column cachedCol = cachedTable.getColumn(i);
                Column col = new Column(cachedCol.getName(), this);
                col.useColInfo(cachedCol);
                this.columns.add(col);
            }
            
            return OperationStatus.SUCCESS;
        }
        
        /* Get the column information from the catalog. */
        if (this.columns == null) {
            OperationStatus ret = Catalog.getMetadata(this);
            if (ret == OperationStatus.NOTFOUND) {
                System.err.println(name + ": no such table");
                return ret;
            }
        }
        
        /* Configure the database and open a handle to it. */
        try {
            DatabaseConfig config = new DatabaseConfig();
            config.setTransactional(true);
            db = DBMS.getEnv().openDatabase(null, this.dbName(), config);
        } catch (DatabaseNotFoundException e) {
            System.err.println(name + ": could not find database");
            return OperationStatus.NOTFOUND;
        }
        
        /* Add this table to the in-memory cache of open tables. */
        tableCache.put(this.name, this);
        
        return OperationStatus.SUCCESS;
    }
    
    /**
     * Closes the table and ensures all changes have been written to disk.
     * It also removes the table from the in-memory cache of open tables.
     * If the <code>Table.cacheClose()</code> method is invoked to close
     * all tables, it is <i>not</i> necessary to call this method as well.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem closing
     *         the underlying database.
     */
    public void close() throws DatabaseException {
        /* Remove the table from the in-memory cache of open tables. */
        Table t = tableCache.remove(this.name);
        
        /* 
         * Close the handle.  Note that we need to use the Table
         * object that was in the cache, because the handle is stored
         * in that object.
         */
        if (t != null && t.db != null) {
            t.db.close();
        }
    }
    
    /**
     * Returns the column (if any) that is the primary key of this table.
     *
     * @return  the primary key column, or null if there is no primary key
     * @throws  RuntimeException if the table object does not have any
     *          columns associated with it
     */
    public Column primaryKeyColumn() {
        if (this.columns == null) {
            throw new RuntimeException("this Table object does not " +
              "have any columns");
        }
        for (int i = 0; i < this.columns.size(); i++) {
            Column col = this.columns.get(i);
            if (col.isPrimaryKey()) {
                return col;
            }
        }
        
        return null;
    }
    
    /**
     * Returns the name of the table's BDB database file.
     *
     * @return  the name of the database file
     */
    public String dbName() {
        return this.name + ".tab.db";
    }
    
    /**
     * Returns the name of the table.
     *
     * @return  the table's name
     */
    public String getName() {
        return this.name;
    }
    
    /**
     * Returns the range variable (if any) associated with this table.
     *
     * @return  the range variable
     */
    public String getRangeVar() {
        return this.rangeVar;
    }
    
    /**
     * Returns the database handle for this table's underlying database.
     *
     * @return  the database handle
     */
    public Database getDB() {
        return this.db;
    }
    
    /**
     * Returns the column with the specified index in this table. 
     * The leftmost column has an index of 0.
     *
     * @return  the column
     * @throws  RuntimeException if the table object does not have any
     *          columns associated with it
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Column getColumn(int i) {
        if (this.columns == null) {
            throw new RuntimeException("this Table object does not " +
              "have any columns");
        }
        return this.columns.get(i);
    }
    
    /**
     * Returns the number of columns in this table.
     *
     * @return  the number of columns
     */
    public int numColumns() {
        return (this.columns == null ? 0 : this.columns.size());
    }
    
    public String toString() {
        return this.name;
    }
    
    /**
     * Sets a range variable (i.e., an alias) for this table.  Used
     * when there is a SELECT command that uses AS to rename a table 
     * in the FROM clause.
     *
     * @param  r  the range variable
     */
    public void setRangeVariable(String r) {
        this.rangeVar = r;
    }
    
    /**
     * Adds a column to the list of columns for this table.
     *
     * @param  c  the column
     */
    public void addColumn(Column c) {
        if (this.columns == null) {
            this.columns = new ArrayList<Column>();
        }
        this.columns.add(c);
    }
    
    /**
     * Sets the database handle for this table's underlying database.
     *
     * @param  db  the database handle
     */
    public void setDB(Database db) {
        this.db = db;
    }
    
    /**
     * Initializes the in-memory cache of open tables.
     */
    public static void cacheInit() {
        tableCache = new Hashtable<String, Table>(TABLE_CACHE_INIT_SIZE);
    }
    
    /**
     * Closes the in-memory cache of open tables, which closes all
     * the BDB database handles for the open tables.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while closing a database handle
     */
    public static void cacheClose() throws DatabaseException {
        if (tableCache != null) {
            /* Close the databases for all tables in the table cache. */
            Iterator<Table> iter = tableCache.values().iterator();
            while (iter.hasNext()) {
                Table t = iter.next();
                
                /* 
                 * Note that we don't call Table.close(), because that
                 * removes the table from the hashtable that we are
                 * currently interating over, which produces in an
                 * exception.
                 */
                if (t != null) {
                    t.db.close();
                }
            }
        }
        
        tableCache = null;
    }
}
