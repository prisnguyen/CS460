/*
 * TrueExpression.java
 *
 * DBMS Implementation
 */

/**
 * A class that represents a conditional expression that is always true.
 * It is used when evaluating SQL statements with empty WHERE clauses.
 */
public class TrueExpression extends ConditionalExpression {
    /**
     * Evaluates the expression represented by the called object,
     * which is always true.
     *
     * @return  always returns true!
     */
    public boolean isTrue() {
        return true;
    }
}
