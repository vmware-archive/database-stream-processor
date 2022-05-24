package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes the schema of a table as produced by a CREATE TABLE DDL statement.
 */
public class TableDDL extends AbstractTable implements ScannableTable, SimulatorResult {
    private final SqlNode node;
    public final String name;
    public final List<ColumnInfo> columns;

    public TableDDL(SqlNode node, String name) {
        this.node = node;
        this.name = name;
        this.columns = new ArrayList<>();
    }

    public void addColumn(ColumnInfo info) {
        this.columns.add(info);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnInfo ci: this.columns) {
            RelDataType type = ci.type;
            builder.add(ci.name, type);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // We don't plan to use this method, but the optimizer requires this API
        throw new NotImplementedException();
    }

    @Override
    public SqlNode getNode() {
        return this.node;
    }
}
