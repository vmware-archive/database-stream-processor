package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains the catalog.
 */
public class Catalog extends AbstractSchema {
    public final String schemaName;
    private final Map<String, Table> tableMap;

    public Catalog(String schemaName) {
        this.schemaName = schemaName;
        this.tableMap = new HashMap<>();
    }

    public static String identifierToString(SqlIdentifier identifier) {
        if (!identifier.isSimple())
            throw new RuntimeException("Not a simple identifier " + identifier);
        return identifier.getSimple();
    }

    public void addTable(String name, Table table) {
        this.tableMap.put(name, table);
    }

    @Override
    public Map<String, Table> getTableMap() {
        return this.tableMap;
    }

    public static String toString(SqlNode node) {
        SqlWriter writer = new SqlPrettyWriter();
        node.unparse(writer, 0, 0);
        return writer.toString();
    }
}
