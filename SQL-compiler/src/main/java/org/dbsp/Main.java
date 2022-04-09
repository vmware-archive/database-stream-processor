package org.dbsp;

import org.apache.calcite.rel.RelRoot;
import org.dbsp.sqlCompiler.CalciteCompiler;


public class Main {
    public static void main(String[] args) throws Exception {
        CalciteCompiler compiler = new CalciteCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT," +
                "COL2 VARCHAR(60)," +
                "COL3 BOOL" +
        ")";

        compiler.compile(ddl);
        String query = "SELECT T.COLUMN3, SUM(T.COLUMN2) FROM T GROUP BY T.COLUMN3";
        RelRoot root = compiler.compile(query);
        System.out.println(root);
        /*
        Table t = new TableNoData();
        System.out.println(t.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)));
        simple.addTable("T", t);
        System.out.println(rootSchema.getTableNames());
        CalciteSchema.TableEntry te = rootSchema.getTable("T", false);
        assert te != null;
        RelRoot root = converter.convertQuery(validatedSqlNode, false, true);
        */
    }
}
