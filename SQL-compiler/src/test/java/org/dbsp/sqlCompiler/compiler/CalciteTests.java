package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.dbsp.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.dbsp.Circuit;
import org.dbsp.util.IndentStringBuilder;
import org.junit.Test;

public class CalciteTests {
    private CalciteCompiler compileDef() throws SqlParseException {
        CalciteCompiler calcite = new CalciteCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT," +
                "COL2 FLOAT," +
                "COL3 BOOLEAN" +
                //"COL4 VARCHAR" +
                ")";

        calcite.compile(ddl);
        return calcite;
    }

    private void compileQuery(CalciteCompiler calcite, String query) throws SqlParseException {
        calcite.compile(query);
        CalciteProgram program = calcite.getProgram();

        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler();
        Circuit dbsp = compiler.compile(program);
        IndentStringBuilder builder = new IndentStringBuilder();
        dbsp.toRustString(builder);
        String result = builder.toString();
        System.out.println(result);
    }

    @Test
    public void projectTest() throws SqlParseException {
        CalciteCompiler calcite = this.compileDef();
        String query = "CREATE VIEW V AS SELECT T.COL3 FROM T";
        this.compileQuery(calcite, query);
    }

    @Test
    public void unionTest() throws SqlParseException {
        CalciteCompiler calcite = this.compileDef();
        String query = "CREATE VIEW V AS (SELECT * FROM T) UNION (SELECT * FROM T)";
        this.compileQuery(calcite, query);
    }

    @Test
    public void whereTest() throws SqlParseException {
        CalciteCompiler calcite = this.compileDef();
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL3";
        this.compileQuery(calcite, query);
    }

    @Test
    public void exceptTest() throws SqlParseException {
        CalciteCompiler calcite = this.compileDef();
        String query = "CREATE VIEW V AS SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.compileQuery(calcite, query);
    }
}
