package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.type.Type;

import javax.annotation.Nullable;

public class DistinctOperator extends Operator {
    public DistinctOperator(@Nullable Object node, Type outputElementType) {
        super(node, "distinct", "", TypeCompiler.makeZSet(outputElementType));
    }
}
