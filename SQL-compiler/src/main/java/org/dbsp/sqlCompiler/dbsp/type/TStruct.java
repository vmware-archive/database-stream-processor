/*
 * Copyright (c) 2021 VMware, Inc.
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
 */

package org.dbsp.sqlCompiler.dbsp.type;

import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;

public class TStruct extends Type {
    private final String name;
    private final List<Field> args;
    private final HashSet<String> fields = new HashSet<String>();

    public TStruct(@Nullable Object node, String name, List<Field> args) {
        super(node,false);
        this.name = name;
        this.args = args;
        for (Field f: args) {
            if (this.hasField(f.getName()))
                this.error("Field name " + f + " is duplicated");
            fields.add(f.getName());
        }
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append(this.name)
                .append("{")
                .join(", ", this.args)
                .append("}");
    }

    @Override
    public Type setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        if (mayBeNull)
            this.error("Nullable structs not supported");
        return this;
    }

    public boolean hasField(String fieldName) {
        return this.fields.contains(fieldName);
    }

    public String getName() { return this.name; }

    public List<Field> getFields() { return this.args; }

    @Override
    public boolean same(Type type) {
        if (!super.same(type))
            return false;
        if (!type.is(TStruct.class))
            return false;
        TStruct other = type.to(TStruct.class);
        if (!this.name.equals(other.name))
            return false;
        if (this.args.size() != other.args.size())
            return false;
        for (int i = 0; i < this.args.size(); i++)
            if (!this.args.get(i).equals(other.args.get(i)))
                return false;
        return true;
    }

    public Type getFieldType(String col) {
        for (Field f : this.getFields()) {
            if (f.getName().equals(col))
                return f.getType();
        }
        this.error("Field " + col + " not present in struct " + this.name);
        throw new RuntimeException("unreachable");
    }
}
