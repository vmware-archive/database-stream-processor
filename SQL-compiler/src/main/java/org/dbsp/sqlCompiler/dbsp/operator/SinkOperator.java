package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class SinkOperator extends Operator {
    public SinkOperator(@Nullable Object node, Type outputType, String outputName) {
        super(node, "inspect", "", outputType, outputName);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder
                .append(this.inputs.get(0).getName())
                .append(".")
                .append(this.operation) // inspect
                .append("(move |m| { *")
                .append(this.getName())
                .append(".borrow_mut() = ")
                .append("m.clone() });");
    }
}
