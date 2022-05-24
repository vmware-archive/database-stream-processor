package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

/**
 * The representation of a CREATE VIEW AS ... DDL statement.
 */
public class ViewDDL implements SimulatorResult {
    private final SqlNode node;
    /**
     * Query defining the view.
     */
    public final SqlNode query;
    /**
     * Name of the view.
     */
    public final String name;
    /**
     * Compiled and optimized query.
     */
    @Nullable
    public RelRoot compiled;

    public ViewDDL(SqlNode node, String name, SqlNode query) {
        this.node = node;
        this.query = query;
        this.name = name;
        this.compiled = null;
    }

    @Override
    public SqlNode getNode() {
        return this.node;
    }

    public void setCompiledQuery(RelRoot compiled) {
        this.compiled = compiled;
    }
}
