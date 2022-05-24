package org.dbsp.sqlCompiler.dbsp.expression;

import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

/**
 * An expression that is a closure of the current row.  The current row is a variable named 't'.
 * In particular, FieldExpressions can refer to the row using this variable.
 */
public class ClosureExpression extends Expression {
    private final Expression expression;

    public ClosureExpression(@Nullable Object node, Type type, Expression expression) {
        super(node, type);
        this.expression = expression;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append("|t| ").append(this.expression);
    }
}
