package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.type.StreamType;
import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class SumOperator extends Operator {
    public SumOperator(@Nullable Object node, Type elementType) {
        super(node, "sum", "", TypeCompiler.makeZSet(elementType));
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        builder.append("let ")
                .append(this.getName())
                .append(": ")
                .append(new StreamType(this.outputType))
                .append(" = ");
        if (!this.inputs.isEmpty())
            builder.append(this.inputs.get(0).getName())
                    .append(".");
        builder.append(this.operation)
                .append("(&[");
        for (int i = 1; i < this.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append(this.inputs.get(i).getName());
        }
        if (!this.function.isEmpty()) {
            if (this.inputs.size() > 1)
                builder.append(",");
            builder.append(this.function);
        }
        return builder.append("]);");
    }
}
