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

package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.*;
import org.dbsp.sqlCompiler.dbsp.type.IHasType;
import org.dbsp.sqlCompiler.dbsp.type.StreamType;
import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.NameGen;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A DBSP operator that applies a function to the inputs and produces an output.
 */
public class Operator extends Node implements IHasName, IHasType {
    final List<Operator> inputs;
    /**
     * Operation that is invoked on inputs; corresponds to a DBSP operator name, e.g., join.
     */
    final String operation;
    /**
     * Rust code that is passed to the DBSP operator name, usually a closure.
     */
    final String function;
    /**
     * Output assigned to this variable.
     */
    final String outputName;
    /**
     * Type of output produced.
     */
    final Type outputType;

    public Operator(@Nullable Object node, String operation, String function, Type outputType, String outputName) {
        super(node);
        this.inputs = new ArrayList<>();
        this.operation = operation;
        this.function = function;
        this.outputName = outputName;
        this.outputType = outputType;
    }

    public Operator(@Nullable Object node, String operation, String function, Type outputType) {
        this(node, operation, function, outputType, new NameGen().toString());
    }

    public void addInput(Operator node) {
        this.inputs.add(node);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        builder.append("let ")
                .append(this.getName())
                .append(": ")
                .append(new StreamType(this.outputType))
                .append(" = ");
        if (!this.inputs.isEmpty())
            builder.append(this.inputs.get(0).getName())
                   .append(".");
        builder.append(this.operation)
                .append("(");
        for (int i = 1; i < this.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append(this.inputs.get(i).getName());
        }
        if (!this.function.isEmpty()) {
            if (this.inputs.size() > 1)
                builder.append(",");
            builder.append(this.function);
        }
        return builder.append(");");
    }

    @Override
    public String getName() {
        return this.outputName;
    }

    @Override
    public Type getType() {
        return this.outputType;
    }
}
