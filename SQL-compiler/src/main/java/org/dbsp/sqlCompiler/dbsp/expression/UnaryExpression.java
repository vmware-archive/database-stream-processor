package org.dbsp.sqlCompiler.dbsp.expression;

import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class UnaryExpression extends Expression {
    private final Expression left;
    private final String operation;

    public UnaryExpression(@Nullable Object node, Type type, String operation, Expression... operands) {
        super(node, type);
        this.operation = operation;
        assert operands.length == 1 : "Expected 1 operand, not " + operands.length;
        this.left = operands[0];
        assert this.left != null : "Null operand";
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append("(")
                .append(this.operation)
                .append(this.left)
                .append(")");
    }
}
