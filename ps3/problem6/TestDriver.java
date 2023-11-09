/*
 * TestDriver
 * 
 * A program that can be used for testing your code for Problem 6.
 */

import java.sql.*;      // needed for the JDBC-related classes 
import java.io.*;       // needed for file-related classes

public class TestDriver {
    public static void main(String[] args) 
        throws ClassNotFoundException, SQLException, FileNotFoundException
    {
        // Add your test code below.
        // MovieToXML xml = new MovieToXML("movie.sqlite");
        // System.out.println(xml.fieldsFor(xml.idFor("Black Panther")));
        // System.out.println(xml.fieldsFor("1234567"));   // no movie with that id
        // System.out.println(xml.fieldsFor(xml.idFor("Cool Hand Luke")));
        
        // MovieToXML xml = new MovieToXML("movie.sqlite");
        // System.out.println(xml.idFor("Black Panther"));
        // System.out.println(xml.actorsFor(xml.idFor("Broadway Melody, The")));
        // System.out.println(xml.actorsFor("1234567"));
        // System.out.println(xml.actorsFor(xml.idFor("Wonder Woman")));

        MovieToXML xml = new MovieToXML("movie.sqlite");
        System.out.println(xml.directorsFor(xml.idFor("Black Panther")));
        System.out.println(xml.directorsFor("1234567"));
        System.out.println(xml.directorsFor(xml.idFor("Frozen")));

        // MovieToXML xml = new MovieToXML("movie.sqlite");
        // System.out.println(xml.elementFor(xml.idFor("Black Panther")));
        // System.out.println(xml.elementFor("1234567"));
        // System.out.println(xml.elementFor(xml.idFor("Wonder Woman")));
    }
}