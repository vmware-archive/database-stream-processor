/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 *
 */

package org.dbsp.sqlCompiler.dbsp;

import org.dbsp.sqlCompiler.dbsp.operator.Operator;
import org.dbsp.sqlCompiler.dbsp.operator.SinkOperator;
import org.dbsp.sqlCompiler.dbsp.operator.SourceOperator;
import org.dbsp.sqlCompiler.dbsp.type.TTuple;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class Circuit extends Node {
    static String rustPreamble = "use dbsp::{\n" +
            "    algebra::{FiniteMap, HasZero, ZSetHashMap},\n" +
            "    circuit::{Root, Runtime, Stream},\n" +
            "    operator::{CsvSource, DelayedFeedback},\n" +
            "};\n" +
            "use ordered_float::OrderedFloat;\n" +
            "type Weight = isize;\n";

    private final List<Operator> inputOperators = new ArrayList<>();
    private final List<Operator> outputOperators = new ArrayList<>();
    private final List<Operator> operators = new ArrayList<>();
    private final String name;

    public Circuit(@Nullable Object node, String name) {
        super(node);
        this.name = name;
    }

    public void addOperator(Operator operator) {
        if (operator instanceof SourceOperator)
            this.inputOperators.add(operator);
        else if (operator instanceof SinkOperator)
            this.outputOperators.add(operator);
        else
            this.operators.add(operator);
    }

    private void genRcRell(IndentStringBuilder builder, Operator op) {
        builder.append("let ")
                .append(op.getName())
                .append(" = Rc::new(RefCell::<")
                .append(op.getType())
                .append(">::new(Default::default()));")
                .newline();
        builder.append("let ")
                .append(op.getName())
                .append("_external = ")
                .append(op.getName())
                .append(".clone();")
                .newline();
        if (op instanceof SourceOperator) {
            builder.append("let ")
                    .append(op.getName())
                    .append(" = Generator::new(move || ")
                    .append(op.getName())
                    .append(".borrow().clone());")
                    .newline();
        }
    }

    /**
     * Generates a Rust function that returns a closure which evaluates the circuit.
     */
    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        builder.append(rustPreamble)
                .newline();

        // function prototype:
        // fn circuit_generator() -> impl FnMut(T0, T1) -> (O0, O1) {
        builder.append("fn ")
                .append(this.name)
                .append("_generator")
                .append("() -> impl FnMut(");

        boolean first = true;
        for (Operator i: this.inputOperators) {
            if (!first)
                builder.append(",");
            first = false;
            builder.append(i.getType());
        }
        builder.append(") -> ");
        TTuple tuple = new TTuple(null, Linq.map(this.outputOperators, Operator::getType));
        builder.append(tuple)
                .append(" {")
                .increase();

        // For each input and output operator a corresponding Rc cell
        for (Operator i: this.inputOperators)
            this.genRcRell(builder, i);

        for (Operator o: this.outputOperators)
            this.genRcRell(builder, o);

        // Circuit body
        builder.append("let root = Root::build(|circuit| {")
                .increase();
        for (Operator i: this.inputOperators)
            builder.append(i)
                    .newline();
        for (Operator op: this.operators)
            op.toRustString(builder)
                    .newline();
        for (Operator i: this.outputOperators)
            builder.append(i)
                    .newline();

        builder.decrease()
                .append("})")
                .append(".unwrap();")
                .newline();

        // Create the closure and return it.
        builder.append("return move |")
                .append(String.join(", ",
                        Linq.map(this.inputOperators, Operator::getName)))
                .append("| {")
                .increase();

        for (Operator i: this.inputOperators)
            builder.append("*")
                    .append(i.getName())
                    .append("_external.borrow_mut() = ")
                    .append(i.getName())
                    .append(";")
                    .newline();
        builder.append("root.step().unwrap();")
                        .newline();
        builder.append("return ");
        if (this.outputOperators.size() > 1)
            builder.append("(");
        builder.append(String.join(", ",
                Linq.map(this.outputOperators,
                        o -> o.getName() + "_external.borrow().clone()")));
        if (this.outputOperators.size() > 1)
            builder.append(")");
        builder.append(";")
                .newline()
                .decrease()
                .append("};")
                .newline()
                .decrease()
                .append("}")
                .newline();
        return builder;
    }

    @Override
    public String toString() {
        IndentStringBuilder builder = new IndentStringBuilder();
        this.toRustString(builder);
        return builder.toString();
    }
}
