/*
 * ConditionalExpression.java
 *
 * DBMS Implementation
 */

/**
 * An abstract class that represents a conditional expression like those
 * found in the WHERE clause of a SQL statement.
 */
public abstract class ConditionalExpression {
    private ConditionalExpression left;
    private ConditionalExpression right;
    
    /**
     * Default constructor used when constructing an object of a
     * subclass that has no subexpressions (e.g., a TrueExpression) or
     * that has subexpressions that are not ConditionalExpressions
     * (e.g., a Comparison).
     */
    public ConditionalExpression() {
        this.left = this.right = null;
    }
    
    /**
     * Constructor used when constructing an object of a subclass that
     * has one or two subexpressions.
     *
     * @param  left  one of the conditional subexpressions
     * @param  right the other conditional subexpression
     */
    public ConditionalExpression(ConditionalExpression left,
                                 ConditionalExpression right) 
    {
        this.left = left;
        this.right = right;
    }
    
    /**
     * Returns the left (i.e., the first) subexpression of the
     * expression represented by the called object.
     *
     * @return  the left subexpression
     */
    public ConditionalExpression getLeft() {
        return this.left;
    }
    
    /**
     * Returns the right (i.e., the second) subexpression of the
     * expression represented by the called object.
     *
     * @return  the right subexpression
     */
    public ConditionalExpression getRight() {
        return this.right;
    }
    
    /**
     * Evaluates the expression represented by the called object, based on
     * the current truth values of the subexpressions.
     *
     * @return  true if the expression is true, and false if it is false!
     */
    public abstract boolean isTrue();
}
