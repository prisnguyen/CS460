/*
 * MovieToXML
 * 
 * A class for objects that are able to convert movie data from the 
 * relational database used in PS 1 to XML.
 */

import java.sql.*;      // needed for the JDBC-related classes
import java.io.*;       // needed for the PrintStream class

public class MovieToXML {
    private Connection db;   // a connection to the database
    
    /*
     * MovieToXML constructor - takes the name of a SQLite file containing
     * a Movie table like the one from PS 1, and creates an object that 
     * can be used to convert the data in that table to XML.
     * 
     * ** YOU SHOULD NOT CHANGE THIS METHOD **
     */
    public MovieToXML(String dbFilename) throws SQLException {
        this.db = DriverManager.getConnection("jdbc:sqlite:" + dbFilename);
    }
    
    /*
     * simpleElem - a private helper method takes the name and value of 
     * a simple XML element and returns a string representation of that 
     * element
     * 
     * ** YOU SHOULD NOT CHANGE THIS METHOD **
     */
    private String simpleElem(String name, String value) {
        String elem = "<" + name + ">";
        elem += value;
        elem += "</" + name + ">";
        return elem;
    }
    
    /*
     * Takes a string representing a SQL query for the movie database
     * and returns a ResultSet object that represents the results
     * of the query (if any).
     * 
     * ** YOU SHOULD NOT CHANGE THIS METHOD **
     */
    public ResultSet resultsFor(String query) throws SQLException {
        Statement stmt = this.db.createStatement();
        ResultSet results = stmt.executeQuery(query);
        return results;
    }

    /*
     * idFor - takes the name of a movie and returns the id number of 
     * that movie in the database as a string. If the movie is not in the 
     * database, it returns an empty string.
     * 
     * ** YOU SHOULD NOT CHANGE THIS METHOD **
     */
    public String idFor(String name) throws SQLException {
        String query = "SELECT id FROM Movie WHERE name = '" + name + "';";
        ResultSet results = resultsFor(query);
        
        if (results.next()) {    
            String id = results.getString(1);
            return id;
        } else {
            return "";
        }
    }   
    
    /*
     * fieldsFor - takes a string representing the id number of a movie
     * and returns a sequence of XML elements for the non-null field values
     * of that movie in the database. If there is no movie with the specified
     * id number, the method returns an empty string.
     */
    public String fieldsFor(String movieID) throws SQLException {
        
        // replace this return statement with your implementation of the method
        String query = "SELECT * FROM Movie WHERE id = '" + movieID + "';";
        ResultSet results = resultsFor(query);

        if (results.next()) {
            String result = "";

            String name = results.getString(2);
            String year = results.getString(3);
            String rating = results.getString(4);
            String runtime = results.getString(5);
            String genre = results.getString(6);
            String earnings_rank = results.getString(7);

            if (name != null) {
                result += "    " + simpleElem("name", name) + "\n";
            }

            if (year != null) {
                result += "    " + simpleElem("year", year) + "\n";
            }

            if (rating != null) {
                result += "    " + simpleElem("rating", rating) + "\n";
            }

            if (runtime != null) {
                result += "    " + simpleElem("runtime", runtime) + "\n";
            }

            if (genre != null) {
                result += "    " + simpleElem("genre", genre) + "\n";
            }

            if (earnings_rank != null) {
                result += "    " + simpleElem("earnings_rank", earnings_rank) + "\n";
            }

            return result;
        } else {
            return "";
        }
    }
    
    /*
     * actorsFor - takes a string representing the id number of a movie
     * and returns a single complex XML element named "actors" that contains a
     * nested child element named "actor" for each actor associated with that
     * movie in the database. If there is no movie with the specified
     * id number, the method returns an empty string.
     */
    public String actorsFor(String movieID) throws SQLException {
        String query = "SELECT p.name FROM Person p " + 
                       "JOIN Actor a ON p.id = a.actor_id " +
                       "WHERE a.movie_id = '" + movieID + "'" +
                       "ORDER BY p.name;";
        ResultSet results = resultsFor(query);
        String result = "";

        while (results.next()) {
            String actor = results.getString(1);
    
            if (actor != null) {
                result += ("      ") + (simpleElem("actor", actor)) + ("\n");
            }
        }
    
        if (!result.isEmpty()) {
            result = "    <actors>\n" + result + "    </actors>\n";
        } else {
            return "";
        }
    
        return result;
    }

    /*
     * directorsFor - takes a string representing the id number of a movie
     * and returns a single complex XML element named "directors" that contains a
     * nested child element named "director" for each director associated with 
     * that movie in the database. If there is no movie with the specified
     * id number, the method returns an empty string.
     */
    public String directorsFor(String movieID) throws SQLException {
        String query = "SELECT p.name FROM Person p " + 
                       "JOIN Director d ON p.id = d.director_id " +
                       "WHERE d.movie_id = '" + movieID + "'" +
                       "ORDER BY p.name;";
        ResultSet results = resultsFor(query);
        String result = "";

        while (results.next()) {
            String directors = results.getString(1);
    
            if (directors != null) {
                result += ("      ") + (simpleElem("director", directors)) + ("\n");
            }
        }
    
        if (!result.isEmpty()) {
            result = "    <directors>\n" + result + "    </directors>\n";
        } else {
            return "";
        }
    
        return result;
    }    
    
    /*
     * elementFor - takes a string representing the id number of a movie
     * and returns a single complex XML element named "movie" that contains
     * nested child elements for all of the fields, actors, and directors 
     * associated with  that movie in the database. If there is no movie with 
     * the specified id number, the method returns an empty string.
     */
    public String elementFor(String movieID) throws SQLException {
        String query = "SELECT id FROM Movie WHERE id = '" + movieID + "';";
        ResultSet results = resultsFor(query);
    
        if (results.next()) {
            String result = "  <movie id=\"" + movieID + "\">\n";
    
            String fields = fieldsFor(movieID);
            if (!fields.isEmpty()) {
                result += fields;
            }
    
            String actors = actorsFor(movieID);
            if (!actors.isEmpty()) {
                result += actors;
            }
    
            String directors = directorsFor(movieID);
            if (!directors.isEmpty()) {
                result += directors;
            }
            result += "  </movie>\n";
    
            return result;
        } else {
            return "";
        }
    }

    /*
     * createFile - creates a text file with the specified filename containing 
     * an XML representation of the entire Movie table.
     * 
     * ** YOU SHOULD NOT CHANGE THIS METHOD **
     */
    public void createFile(String filename) 
      throws FileNotFoundException, SQLException 
    {
        PrintStream outfile = new PrintStream(filename);    
        outfile.println("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>");
        outfile.println("<movies>");
        
        // Use a query to get all of the ids from the Movie Table.
        ResultSet results = resultsFor("SELECT id FROM Movie;");
        
        // Process one movie id at a time, creating its 
        // XML element and writing it to the output file.
        while (results.next()) {
            String movieID = results.getString(1);
            outfile.println(elementFor(movieID));
        }
        
        outfile.println("</movies>");
        
        // Close the connection to the output file.
        outfile.close();
        System.out.println("movies.xml has been written.");
    }
    
    /*
     * closeDB - closes the connection to the database that was opened when 
     * the MovieToXML object was constructed
     * 
     * ** YOU SHOULD NOT CHANGE THIS METHOD **
     */
    public void closeDB() throws SQLException {
        this.db.close();
    }

    /*** YOU SHOULD NOT CHANGE THIS METHOD ***/
    public static void main(String[] args) 
        throws ClassNotFoundException, SQLException, FileNotFoundException
    {
        MovieToXML xml = new MovieToXML("movie.sqlite");
        xml.createFile("movies.xml");
        xml.closeDB();
    }
}
