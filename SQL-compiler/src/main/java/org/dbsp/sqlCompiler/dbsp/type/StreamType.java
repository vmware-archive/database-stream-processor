package org.dbsp.sqlCompiler.dbsp.type;

import org.dbsp.util.IndentStringBuilder;

public class StreamType extends Type {
    public final Type elementType;

    public StreamType(Type elementType) {
        super(elementType.getNode(), elementType.mayBeNull);
        this.elementType = elementType;
    }

    @Override
    public Type setMayBeNull(boolean mayBeNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append("Stream<")
                .append("_, ") // Circuit type
                .append(this.elementType)
                .append(">");
    }
}
