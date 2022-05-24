package org.dbsp.sqlCompiler.dbsp;

import org.dbsp.util.IdGen;
import org.dbsp.util.Printable;

import javax.annotation.Nullable;

/**
 * Base interface for all DBSP nodes.
 * The Printable interface implements output as a Rust string.
 */
public abstract class Node
        extends IdGen
        implements Printable, INode {

    /**
     * Original query Sql node that produced this node.
     */
    private final @Nullable
    Object node;

    protected Node(@Nullable Object node) {
        this.node = node;
    }

    @Nullable
    public Object getNode() { return this.node; }
}
