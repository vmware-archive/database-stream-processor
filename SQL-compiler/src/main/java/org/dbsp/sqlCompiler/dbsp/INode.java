package org.dbsp.sqlCompiler.dbsp;

import org.dbsp.util.ICastable;
import org.dbsp.util.TranslationException;

import javax.annotation.Nullable;

public interface INode extends ICastable {
    default <T> T checkNull(@Nullable T value) {
        if (value == null)
            this.error("Null pointer");
        assert value != null;
        return value;
    }

    default <T> boolean is(Class<T> clazz) {
        return this.as(clazz) != null;
    }

    default void error(String message) {
        throw new TranslationException(message, this.getNode());
    }

    /**
     * This is the SQL IR node that was compiled to produce
     * this DDlogIR node.
     * @return
     */
    @Nullable
    public Object getNode();
}
