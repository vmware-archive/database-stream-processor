package org.dbsp.sqlCompiler;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains the catalog.
 */
public class SimpleSchema extends AbstractSchema {
    public final String schemaName;
    private final Map<String, Table> tableMap;

    public SimpleSchema(String schemaName) {
        this.schemaName = schemaName;
        this.tableMap = new HashMap<>();
    }

    public void addTable(String name, Table table) {
        this.tableMap.put(name, table);
    }

    @Override
    public Map<String, Table> getTableMap() {
        return this.tableMap;
    }
}
