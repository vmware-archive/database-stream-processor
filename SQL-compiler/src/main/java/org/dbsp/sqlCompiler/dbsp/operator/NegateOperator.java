package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.type.Type;

import javax.annotation.Nullable;

public class NegateOperator extends Operator {
    public NegateOperator(@Nullable Object node, Type outputType) {
        super(node, "neg", "", outputType);
    }
}
