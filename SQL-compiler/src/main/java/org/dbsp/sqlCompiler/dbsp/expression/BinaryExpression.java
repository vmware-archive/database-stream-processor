package org.dbsp.sqlCompiler.dbsp.expression;

import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class BinaryExpression extends Expression {
    private final Expression left;
    private final Expression right;
    private final String operation;

    public BinaryExpression(@Nullable Object node, Type type, String operation, Expression... operands) {
        super(node, type);
        this.operation = operation;
        assert operands.length == 2 : "Expected 2 operands, not " + operands.length;
        this.left = operands[0];
        this.right = operands[1];
        assert this.left != null : "Null left operand";
        assert this.right != null : "Null right operand";
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append("(")
                .append(this.left)
                .append(" ")
                .append(this.operation)
                .append(" ")
                .append(this.right)
                .append(")");
    }
}
