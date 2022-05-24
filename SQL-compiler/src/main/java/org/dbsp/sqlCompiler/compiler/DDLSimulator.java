package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.util.Unimplemented;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Simulate the execution of a SQL DDL statement.
 */
public class DDLSimulator {
    final Catalog schema;
    RelDataTypeSystem system = RelDataTypeSystem.DEFAULT;

    public DDLSimulator(Catalog schema) {
        this.schema = schema;
    }

    // TODO: should use SqlTypeUtils.deriveType, but could not figure
    // out how to create a proper validator.
    RelDataType convertType(SqlDataTypeSpec spec) {
        SqlTypeNameSpec type = spec.getTypeNameSpec();
        String str = Catalog.toString(spec);

        if (type instanceof SqlBasicTypeNameSpec) {
            SqlBasicTypeNameSpec basic = (SqlBasicTypeNameSpec) type;
            // This is just insane, there is no way to get to basic.sqlTypeName!
            if (str.equals("INTEGER"))
                return new BasicSqlType(system, SqlTypeName.INTEGER);
            if (str.equals("BOOLEAN"))
                return new BasicSqlType(system, SqlTypeName.BOOLEAN);
            if (str.startsWith("VARCHAR"))
                return new BasicSqlType(system, SqlTypeName.VARCHAR);
            if (str.equals("FLOAT"))
                return new BasicSqlType(system, SqlTypeName.FLOAT);
        }
        throw new Unimplemented("Unknown SQL type: " + str, true);
    }

    List<ColumnInfo> getColumnTypes(SqlNodeList list) {
        List<ColumnInfo> result = new ArrayList<>();
        for (SqlNode col: Objects.requireNonNull(list)) {
            if (col.getKind().equals(SqlKind.COLUMN_DECL)) {
                SqlColumnDeclaration cd = (SqlColumnDeclaration)col;
                RelDataType type = this.convertType(cd.dataType);
                ColumnInfo ci = new ColumnInfo(Catalog.identifierToString(cd.name), type,
                        Objects.requireNonNull(cd.dataType.getNullable()));
                result.add(ci);
                continue;
            }
            throw new Unimplemented(col);
        }
        return result;
    }

    SimulatorResult execute(SqlNode node) {
        SqlKind kind = node.getKind();
        if (kind == SqlKind.CREATE_TABLE) {
            SqlCreateTable ct = (SqlCreateTable)node;
            String tableName = Catalog.identifierToString(ct.name);
            TableDDL table = new TableDDL(node, tableName);
            List<ColumnInfo> cols = this.getColumnTypes(Objects.requireNonNull(ct.columnList));
            cols.forEach(table::addColumn);
            this.schema.addTable(tableName, table);
            return table;
        } else if (kind == SqlKind.CREATE_VIEW) {
            SqlCreateView cv = (SqlCreateView) node;
            return new ViewDDL(node, Catalog.identifierToString(cv.name), cv.query);
        }
        throw new Unimplemented(node);
    }
}
