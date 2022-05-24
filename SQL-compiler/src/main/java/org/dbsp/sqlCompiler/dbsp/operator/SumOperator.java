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

import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.type.StreamType;
import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class SumOperator extends Operator {
    public SumOperator(@Nullable Object node, Type elementType) {
        super(node, "sum", "", TypeCompiler.makeZSet(elementType));
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
                .append("(&[");
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
        return builder.append("]);");
    }
}
