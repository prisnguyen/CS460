/*
 * AndExpression.java
 *
 * DBMS Implementation
 */

/**
 * A class that represents a conditional expression involving an AND operator.
 * An AndExpression involves two conditional subexpressions, and it is true
 * when both of the subexpressions are true, and false otherwise.
 */
public class AndExpression extends ConditionalExpression {
    /**
     * Constructs an AndExpression object that represents the conditional
     * expression <code>left AND right</code>.
     *
     * @param  left  one of the conditional subexpressions being ANDed
     * @param  right the other conditional subexpression
     */
    public AndExpression(ConditionalExpression left,
                         ConditionalExpression right) {
        super(left, right);
    }
    
    /**
     * Evaluates the expression represented by the called object, based on
     * the current truth values of the subexpressions.
     *
     * @return  true if the two subexpressions are both true, and false
     *          otherwise
     */
    public boolean isTrue() {
        return (this.getLeft().isTrue() && this.getRight().isTrue());
    }
}
