package org.dbsp.sqlCompiler.dbsp.type;

import javax.annotation.Nullable;

public class ZSetType extends TUser {
    public ZSetType(@Nullable Object node, Type elementType, Type weightType) {
        super(node, "ZSetHashMap", false, elementType, weightType);
    }
}
