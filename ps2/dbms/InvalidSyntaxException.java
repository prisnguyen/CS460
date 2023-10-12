/*
 * InvalidSyntaxException.java
 *
 * DBMS Implementation
 */

/** 
 * A special exception class that allows us to distinguish parser
 * syntax errors from other exceptions.  Since the parser prints its
 * own error messages, when the DBMS catches this type of exception,
 * it does nothing.
 */
public class InvalidSyntaxException extends RuntimeException {
}
