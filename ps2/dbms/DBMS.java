/*
 * DBMS.java
 *
 * DBMS Implementation
 */

import java.io.*;
import java.util.*;
import com.sleepycat.je.*;

/**
 * The main class of a simple relational database.  It includes the
 * main() method, and methods for performing tasks or obtaining state
 * involving the DBMS application as a whole (e.g., initialization,
 * configuring the environment, aborting the application, etc.)
 *
 * To allow access to the DBMS methods from all other classes, we make
 * all methods static, so that the class name can be used to invoke them.
 *
 * To run the application, enter the command<br>
 * <br>
 * <code>java DBMS</code><br>
 * <br>
 * from the command line.
 */
public class DBMS {
    /** Set this to true to print debugging messages, and false to 
      omit them. */
    public static final boolean DEBUG = true;
    
    /** The home directory of the BDB environment. */
    public static final String DB_HOME = "db";
    
    private static Environment env;
    
    private static boolean hasShutDown = false;
    
    /**
     * The main method for the DBMS application.
     */
    public static void main(String[] args) throws IOException {
        Scanner console = new Scanner(System.in);
        
        init();
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (!hasShutDown) {
                    try {
                        shutdown();
                    } catch (DatabaseException e) {
                        System.err.println("encountered exception shutting down: " + e);
                        e.printStackTrace();
                    }
                }
            }
        });
        
        while (true) {
            try {
                /* Get the command string from the user. */
                System.out.println();
                System.out.println("Enter command (q to quit): ");
                String commandString = console.nextLine();
                
                if (commandString.equalsIgnoreCase("q")) {
                    break;
                }
                
                /* Parse the command string. */
                StringReader commandStream = new StringReader(commandString);
                Lexer l = new Lexer(commandStream);
                Parser p = new Parser(l);
                SQLStatement command = (SQLStatement)p.parse().value;
                if (DEBUG) {
                    System.out.println(command);
                }
                
                /* Execute the SQL command. */
                command.execute();
            } catch (IllegalArgumentException e) {
                System.err.println(e);
            } catch (InvalidSyntaxException e) {
                // error message will have been printed by the parser
            } catch (NoSuchElementException e) {
                // end-of-file was received by the Scanner
                break;
            } catch (Exception e) {
                System.err.println("unexpected exception: " + e);
                e.printStackTrace();
                abort();
            }
        }
        
        try {
            shutdown();
        } catch (DatabaseException e) {
            System.err.println("encountered exception shutting down normally: " + e);
            e.printStackTrace();
        }

        console.close();
    }
    
    /**
     * Initializes the DBMS -- initializing the underlying BDB environment,
     * the catalog, and the in-memory cache of open tables.
     */
    public static void init() {
        try {
            environmentInit();
            Catalog.open();
            Table.cacheInit();
        } catch (Exception e) {
            System.err.println("encountered exception while initializing: " + e);
            e.printStackTrace();
            abort();
        }
    }
    
    /**
     * Prepares the DBMS to shutdown -- closing all open tables, as well
     * as the catalog and the underlying BDB environment.
     * 
     * @throws  DatabaseException if Berkeley DB encounters a problem when
     *          aborting transactions, closing the per-table databases,
     *          or closing the catalog database
     */
    public static void shutdown() throws DatabaseException {
        Table.cacheClose();
        Catalog.close();
        if (env != null) {
            env.close();
        }
        
        hasShutDown = true;
    }
    
    /**
     * Attempts to immediately shut down the database and exit. This method
     * tries to abort all transactions and close all Berkeley DB databases
     * using DBMS.shutdown(). If DBMS.shutdown() fails, an error message
     * is printed. In all cases, the program exits.
     */
    public static void abort() {
        try {
            shutdown();
        } catch (DatabaseException e) {
            System.err.println("encountered exception while aborting: " + e);
            e.printStackTrace();
        }
        
        System.exit(1);
    }
    
    /**
     * Returns the handle for the underlying BDB environment.
     *
     * @return  the environment handle
     */
    public static Environment getEnv() {
        return env;
    }
    
    /**
     * Configures and opens the handle for the underlying DB environment
     *
     * @throws  DatabaseException if Berkeley DB encounters a problem in
     *          opening the environment handle
     * @throws  FileNotFoundException if the Berkeley DB cannot access the
     *          environment's home directory
     * @throws  RuntimeException if the environment's home directory
     *          cannot be created
     * @throws  IllegalStateException if there is an existing file (not a
     *          directory) with the name of the home directory
     */
    private static void environmentInit()
        throws DatabaseException, FileNotFoundException
    {
        /* Create the DB home directory, if necessary. */
        File home = new File(DB_HOME);
        if (!home.exists()) {
            if (!home.mkdir()) {
                throw new RuntimeException("could not create home directory");
            }
        } else if (!home.isDirectory()) {
            throw new IllegalStateException("preexisting " + DB_HOME +
                                            " is not a directory");
        }
        
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(home, envConfig);
    }
}
