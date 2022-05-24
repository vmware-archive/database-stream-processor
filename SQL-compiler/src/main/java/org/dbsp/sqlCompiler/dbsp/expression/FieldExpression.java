package org.dbsp.sqlCompiler.dbsp.expression;

import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class FieldExpression extends Expression {
    private final int fieldNo;

    public FieldExpression(@Nullable RexNode node, int fieldNo, Type type) {
        super(node, type);
        this.fieldNo = fieldNo;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append("|t| t.").append(this.fieldNo);
    }
}
