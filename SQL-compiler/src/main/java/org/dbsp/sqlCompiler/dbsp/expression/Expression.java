package org.dbsp.sqlCompiler.dbsp.expression;

import org.dbsp.sqlCompiler.dbsp.Node;
import org.dbsp.sqlCompiler.dbsp.type.IHasType;
import org.dbsp.sqlCompiler.dbsp.type.Type;

import javax.annotation.Nullable;

public abstract class Expression extends Node implements IHasType {
    private final Type type;

    protected Expression(@Nullable Object node, Type type) {
        super(node);
        this.type = type;
    }

    @Override
    public Type getType() {
        return this.type;
    }
}
