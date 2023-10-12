/*
 * OrExpression.java
 *
 * DBMS Implementation
 */

/**
 * A class that represents a conditional expression involving an OR operator.
 * An OrExpression involves two conditional subexpressions, and it is true
 * when one or both of the subexpressions are true, and false when both of
 * the subexpressions are false.
 */
public class OrExpression extends ConditionalExpression {
    /**
     * Constructs an OrExpression object that represents the conditional
     * expression <code>left OR right</code>.
     *
     * @param  left  one of the conditional subexpressions being ORed
     * @param  right the other conditional subexpression
     */
    public OrExpression(ConditionalExpression left,
                        ConditionalExpression right) {
        super(left, right);
    }
    
    /**
     * Evaluates the expression represented by the called object, based on
     * the current truth values of the subexpressions.
     *
     * @return  true if one or both of the subexpressions are true, and 
     *          false if they are both false
     */
    public boolean isTrue() {
        return (this.getLeft().isTrue() || this.getRight().isTrue());
    }
}
