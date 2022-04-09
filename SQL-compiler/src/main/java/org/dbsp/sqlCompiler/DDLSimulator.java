package org.dbsp.sqlCompiler;

import org.apache.calcite.sql.*;
import org.dbsp.util.Unimplemented;

/**
 * Simulate the execution of a SQL DDL statement.
 */
public class DDLSimulator {
    public DDLSimulator() {
    }

    void execute(SqlNode node) {
        if (node.getKind().equals(SqlKind.CREATE_TABLE)) {
            SqlCall call = (SqlCall)node;

        }
        throw new Unimplemented();
    }
}
