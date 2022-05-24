package org.dbsp.sqlCompiler.dbsp;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.*;
import org.dbsp.sqlCompiler.dbsp.expression.Expression;
import org.dbsp.sqlCompiler.dbsp.operator.*;
import org.dbsp.sqlCompiler.dbsp.type.*;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

public class CalciteToDBSPCompiler extends RelVisitor {
    static class Context {
        @Nullable
        RelNode parent;
        int inputNo;

        public Context(@Nullable RelNode parent, int inputNo) {
            this.parent = parent;
            this.inputNo = inputNo;
        }
    }

    private final Circuit circuit;
    boolean debug = true;
    // The path in the IR tree used to reach the current node.
    final List<Context> stack;
    // Map an input or output name to the corresponding operator
    final Map<String, Operator> ioOperator;
    // Map a RelNode operator to its DBSP implementation.
    final Map<RelNode, Operator> nodeOperator;

    TypeCompiler typeCompiler = new TypeCompiler();
    ExpressionCompiler expressionCompiler = new ExpressionCompiler(true);

    public CalciteToDBSPCompiler() {
        this.circuit = new Circuit(null, "circuit");
        this.stack = new ArrayList<>();
        this.ioOperator = new HashMap<>();
        this.nodeOperator = new HashMap<>();
    }

    private Type convertType(RelDataType dt) {
        return this.typeCompiler.convertType(dt);
    }

    private Type makeZSet(Type type) {
        return TypeCompiler.makeZSet(type);
    }

    Circuit getProgram() {
        return Objects.requireNonNull(this.circuit);
    }

    <T> boolean visitIfMatches(RelNode node, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(node, clazz);
        if (value != null) {
            if (debug)
                System.out.println("Processing " + node);
            method.accept(value);
            return true;
        }
        return false;
    }

    public void visitScan(LogicalTableScan scan) {
        List<String> name = scan.getTable().getQualifiedName();
        String tname = name.get(name.size() - 1);
        Operator op = Utilities.getExists(this.ioOperator, tname);
        this.assignOperator(scan, op);
    }

    void assignOperator(RelNode rel, Operator op) {
        Utilities.putNew(this.nodeOperator, rel, op);
        if (!(op instanceof SourceOperator))
            // These are already added
            this.circuit.addOperator(op);
    }

    Operator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    public void visitProject(LogicalProject project) {
        RelNode input = project.getInput();
        Operator opinput = this.getOperator(input);
        Type type = this.convertType(project.getRowType());
        List<Integer> projectColumns = new ArrayList<>();
        for (RexNode column : project.getProjects()) {
            RexInputRef in = ICastable.as(column, RexInputRef.class);
            assert in != null : "Unhandled columnn reference in project: " + column;
            projectColumns.add(in.getIndex());
        }
        RelProjectOperator op = new RelProjectOperator(project, projectColumns, type);
        op.addInput(opinput);
        this.circuit.addOperator(op);

        DistinctOperator d = new DistinctOperator(project, type);
        d.addInput(op);
        this.assignOperator(project, d);
    }

    private void visitUnion(LogicalUnion union) {
        Type type = this.convertType(union.getRowType());
        SumOperator sum = new SumOperator(union, type);
        for (RelNode input : union.getInputs()) {
            Operator opin = this.getOperator(input);
            sum.addInput(opin);
        }

        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.circuit.addOperator(sum);
            DistinctOperator d = new DistinctOperator(union, type);
            d.addInput(sum);
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        Type type = this.convertType(minus.getRowType());
        SumOperator sum = new SumOperator(minus, type);
        boolean first = true;
        for (RelNode input : minus.getInputs()) {
            Operator opin = this.getOperator(input);

            if (!first) {
                NegateOperator neg = new NegateOperator(minus, type);
                this.circuit.addOperator(neg);
                neg.addInput(opin);
                sum.addInput(neg);
            } else {
                sum.addInput(opin);
            }
        }

        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            this.circuit.addOperator(sum);
            DistinctOperator d = new DistinctOperator(minus, type);
            d.addInput(sum);
            this.assignOperator(minus, d);
        }
    }

    public void visitFilter(LogicalFilter filter) {
        Type type = this.convertType(filter.getRowType());
        Expression condition = this.expressionCompiler.compile(filter.getCondition());
        FilterOperator fop = new FilterOperator(filter, condition, type);
        Operator input = this.getOperator(filter.getInput());
        fop.addInput(input);
        this.assignOperator(filter, fop);
    }

    @Override public void visit(
            RelNode node, int ordinal,
            @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
        stack.add(new Context(parent, ordinal));
        if (debug)
            System.out.println("Visiting " + node);
        // First process children
        super.visit(node, ordinal, parent);
        // Synthesize current node
        boolean success =
                this.visitIfMatches(node, LogicalTableScan.class, this::visitScan) ||
                this.visitIfMatches(node, LogicalProject.class, this::visitProject) ||
                this.visitIfMatches(node, LogicalUnion.class, this::visitUnion) ||
                this.visitIfMatches(node, LogicalMinus.class, this::visitMinus) ||
                this.visitIfMatches(node, LogicalFilter.class, this::visitFilter);
        if (!success)
            throw new Unimplemented(node);
        assert stack.size() > 0 : "Empty stack";
        stack.remove(stack.size() - 1);
    }

    public Circuit compile(CalciteProgram program) {
        for (TableDDL i: program.inputTables) {
            SourceOperator si = this.createInput(i);
            this.circuit.addOperator(si);
        }
        for (ViewDDL view: program.views) {
            SinkOperator o = this.createOutput(view);
            this.circuit.addOperator(o);
            RelNode rel = Objects.requireNonNull(view.compiled).rel;
            this.go(rel);
            // TODO: connect the result of the query compilation with
            // the fields of rel; for now we assume that these are 1/1
            Operator op = this.getOperator(rel);
            o.addInput(op);
        }
        return this.getProgram();
    }

    static Type weightType = new TUser(null, "Weight", false);

    private SourceOperator createInput(TableDDL i) {
        List<Type> fields = new ArrayList<>();
        for (ColumnInfo col: i.columns) {
            Type ftype = this.convertType(col.type);
            fields.add(ftype);
        }
        TTuple type = new TTuple(i, fields);
        SourceOperator result = new SourceOperator(i, this.makeZSet(type), i.name);
        return Utilities.putNew(this.ioOperator, i.name, result);
    }

    private SinkOperator createOutput(ViewDDL v) {
        List<Type> fields = new ArrayList<>();
        assert v.compiled != null;
        for (RelDataTypeField field: v.compiled.validatedRowType.getFieldList()) {
            Type ftype = this.convertType(field.getType());
            fields.add(ftype);
        }
        TTuple type = new TTuple(v, fields);
        SinkOperator result = new SinkOperator(v, this.makeZSet(type), v.name);
        return Utilities.putNew(this.ioOperator, v.name, result);
    }
}
