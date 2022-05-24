package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Describes information about a column in a SQL table.
 */
public class ColumnInfo {
    public final String name;
    public final RelDataType type;
    public final boolean nullable;

    public ColumnInfo(String name, RelDataType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    // TODO: I don't know how to manufacture a proper validator.
    private ColumnInfo(SqlColumnDeclaration decl, SqlValidator validator, boolean nullable) {
        this.name = Catalog.identifierToString(decl.name);
        this.type = decl.dataType.deriveType(validator);
        this.nullable = nullable;
    }
}
