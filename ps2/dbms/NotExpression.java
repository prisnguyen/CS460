/*
 * NotExpression.java
 *
 * DBMS Implementation
 */

/**
 * A class that represents a conditional expression involving a NOT operator.
 * A NotExpression involves one conditional subexpression, and it is true
 * when the subexpression is false, and false when the subexpression is true.
 */
public class NotExpression extends ConditionalExpression {
    /**
     * Constructs a NotExpression object that represents the conditional
     * expression <code>NOT expr</code>.
     *
     * @param  expr  the conditional subexpression being negated
     */
    public NotExpression(ConditionalExpression expr) {
        super(expr, null);
    }
    
    /**
     * Evaluates the expression represented by the called object, based on
     * the current truth value of the subexpression.
     *
     * @return  true if the subexpression is false, and false if the 
     *          subexpression is true
     */
    public boolean isTrue() {
        return (! this.getLeft().isTrue());
    }
}
