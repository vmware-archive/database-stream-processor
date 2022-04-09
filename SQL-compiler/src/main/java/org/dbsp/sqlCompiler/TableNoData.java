package org.dbsp.sqlCompiler;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

/**
 * These tables hold no data, they exist just for the schema.
 */
public class TableNoData extends AbstractTable implements ScannableTable {
    final List<ColumnInfo> columns;

    public TableNoData() {
        this.columns = new ArrayList<>();
    }

    void addColumn(ColumnInfo info) {
        this.columns.add(info);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnInfo ci: this.columns) {
            RelDataType type = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(ci.type), ci.nullable);
            builder.add(ci.name, type);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // We don't plan to use this method, but the optimizer requires this API
        throw new NotImplementedException();
    }
}
