package org.dbsp.sqlCompiler.dbsp.expression;

import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class LiteralExpression extends Expression {
    private final String value;

    public LiteralExpression(@Nullable Object node, Type type, String value) {
        super(node, type);
        this.value = value;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append(this.value);
    }
}
