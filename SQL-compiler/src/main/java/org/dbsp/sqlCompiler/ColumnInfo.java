package org.dbsp.sqlCompiler;

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Describes information about a column in a SQL table.
 */
public class ColumnInfo {
    public final String name;
    public final SqlTypeName type;
    public final boolean nullable;

    ColumnInfo(String name, SqlTypeName type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }
}
