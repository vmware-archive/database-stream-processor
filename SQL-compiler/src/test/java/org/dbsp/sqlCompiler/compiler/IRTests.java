package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.dbsp.*;
import org.dbsp.sqlCompiler.dbsp.operator.Operator;
import org.dbsp.sqlCompiler.dbsp.operator.SinkOperator;
import org.dbsp.sqlCompiler.dbsp.operator.SourceOperator;
import org.dbsp.sqlCompiler.dbsp.type.TSigned;
import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.sqlCompiler.dbsp.type.ZSetType;
import org.junit.Assert;
import org.junit.Test;

public class IRTests {
    @Test
    public void irTest() {
        Circuit circuit = new Circuit(null, "test_scalar");
        Type type = TSigned.signed32;
        Operator input = new SourceOperator(null, type, "i");
        circuit.addOperator(input);
        Operator op = new Operator(null, "apply", "|x| x + 1", type, "op");
        op.addInput(input);
        circuit.addOperator(op);
        Operator output = new SinkOperator(null, type, "o");
        output.addInput(op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }

    @Test
    public void setTest() {
        Circuit circuit = new Circuit(null, "test_zset");
        Type type = new ZSetType(null, TSigned.signed32, TSigned.signed64);
        Operator input = new SourceOperator(null, type, "i");
        circuit.addOperator(input);
        Operator op = new Operator(null, "apply", "|x| x.add_by_ref(&x)", type, "op");
        op.addInput(input);
        circuit.addOperator(op);
        Operator output = new SinkOperator(null, type, "o");
        output.addInput(op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }
}
