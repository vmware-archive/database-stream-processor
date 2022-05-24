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

import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class TTuple extends Type {
    final Type[] tupArgs;

    public static TTuple emptyTupleType = new TTuple(null);

    private TTuple(@Nullable Object node, boolean mayBeNull, Type... tupArgs) {
        super(node, mayBeNull);
        this.tupArgs = tupArgs;
    }

    public TTuple(@Nullable Object node, Type... tupArgs) {
        this(node, false, tupArgs);
    }

    public TTuple(@Nullable Object node, List<Type> tupArgs) {
        this(node, tupArgs.toArray(new Type[0]));
    }

    public int size() {
        return this.tupArgs.length;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        if (this.tupArgs.length == 1)
            return builder.append(this.tupArgs[0]);
        builder.append("(");
        builder.join(", ", this.tupArgs);
        return builder.append(")");
    }

    @Override
    public Type setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new TTuple(this.getNode(), mayBeNull, this.tupArgs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TTuple that = (TTuple) o;
        return Arrays.equals(tupArgs, that.tupArgs);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tupArgs);
    }

    @Override
    public boolean same(Type type) {
        if (!super.same(type))
            return false;
        if (!type.is(TTuple.class))
            return false;
        TTuple other = type.to(TTuple.class);
        if (this.tupArgs.length != other.tupArgs.length)
            return false;
        for (int i = 0; i < this.tupArgs.length; i++)
            if (!this.tupArgs[i].same(other.tupArgs[i]))
                return false;
        return true;
    }

    public Type component(int index) {
        return this.tupArgs[index];
    }
}
