package org.dbsp.sqlCompiler;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Properties;

/**
 * This class wraps up the Calcite SQL compiler.
 * It provides a simple service: Given a sequence of SQL query strings,
 * including a complete set of Data Definition Language statements,
 * it returns a Calcite representation for each statement.
 */
public class CalciteCompiler {
    private final SqlParser.Config parserConfig;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;

    // Adapted from https://www.querifylabs.com/blog/assembling-a-query-optimizer-with-apache-calcite
    public CalciteCompiler() {
        Properties connConfigProp = new Properties();
        connConfigProp.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        connConfigProp.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        connConfigProp.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(connConfigProp);

        this.parserConfig = SqlParser.config().withParserFactory(SqlBabelParserImpl.FACTORY);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        SimpleSchema simple = new SimpleSchema("schema");
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(simple.schemaName, simple);
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema, Collections.singletonList(simple.schemaName), typeFactory, connectionConfig);

        SqlOperatorTable operatorTable = SqlOperatorTables.chain(
                SqlStdOperatorTable.instance()
        );

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withConformance(connectionConfig.conformance())
                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                .withIdentifierExpansion(true);

        this.validator = SqlValidatorUtil.newValidator(
                operatorTable,
                catalogReader,
                typeFactory,
                validatorConfig
        );
        RelOptPlanner planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(connectionConfig)
        );
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(
                planner,
                new RexBuilder(typeFactory)
        );

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);
        this.converter = new SqlToRelConverter(
                null,
                this.validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    /**
     * Given a SQL statement returns a SqlNode - a calcite AST
     * representation of the query.
     * @param sql  SQL query to compile
     */
    private SqlNode parse(String sql) throws SqlParseException {
        // This is a weird API - the parser depends on the query string!
        SqlParser sqlParser = SqlParser.create(sql, this.parserConfig);
        return sqlParser.parseStmt();
    }

    /**
     * Simulate the execution of a DDL statement.
     */
    private void simulate(SqlNode node) {
        DDLSimulator simulator = new DDLSimulator();
        simulator.execute(node);
    }

    /**
     * Given a SQL query statement return a Calcite IR representation
     * of the query.  If the statement is a DDL statement, updates
     * the "state" of the database, and returns null.
     * @param sqlStatement  SQL statement to compile.
     */
    @Nullable
    public RelRoot compile(String sqlStatement) throws SqlParseException {
        SqlNode node = this.parse(sqlStatement);
        if (SqlKind.DDL.contains(node.getKind())) {
            this.simulate(node);
            return null;
        }
        SqlNode validated = this.validator.validate(node);
        return this.converter.convertQuery(validated, false, true);
    }
}
