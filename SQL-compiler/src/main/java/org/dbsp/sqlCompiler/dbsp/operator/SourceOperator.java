package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class SourceOperator extends Operator {
    public SourceOperator(@Nullable Object node, Type outputType, String name) {
        super(node, "", "", outputType, name);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append("let ")
                .append(this.getName())
                .append(" = ")
                .append("circuit.add_source(")
                .append(this.outputName)
                .append(");");
    }
}
