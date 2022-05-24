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

package org.dbsp.sqlCompiler.dbsp.type;

import org.dbsp.sqlCompiler.dbsp.Node;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public abstract class Type extends Node {
    /**
     * True if this type may include null values.
     */
    public final boolean mayBeNull;

    protected Type(@Nullable Object node, boolean mayBeNull) {
        super(node);
        this.mayBeNull = mayBeNull;
    }

    protected Type(boolean mayBeNull) {
        super(null);
        this.mayBeNull = mayBeNull;
    }

    IndentStringBuilder wrapOption(IndentStringBuilder builder, String type) {
        if (this.mayBeNull)
            return builder.append("Option<" + type + ">");
        return builder.append(type);
    }

    public boolean same(Type other) {
        return this.mayBeNull == other.mayBeNull;
    }

    public IsNumericType toNumeric() {
        return this.as(IsNumericType.class, "Expected as numeric type");
    }

    /**
     * Return a copy of this type with the mayBeNull bit set to the specified value.
     * @param mayBeNull  Value for the mayBeNull bit.
     */
    public abstract Type setMayBeNull(boolean mayBeNull);

    /**
     * True if the given type is a numeric type.
     * @param type  Type to analyze.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isNumeric(Type type) {
        return type instanceof IsNumericType;
    }

    /**
     * Get the None{} value of the option type corresponding to this type.
    public DBSPExpression getNone(@Nullable SqlNode node) {
        return new DBSPENull(node, this.setMayBeNull(true));
    }
     */

    public boolean isBaseType() {
        return this.is(IBaseType.class);
    }
}
