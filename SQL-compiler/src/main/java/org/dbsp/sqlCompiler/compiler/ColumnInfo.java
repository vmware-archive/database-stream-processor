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

package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Describes information about a column in a SQL table.
 */
public class ColumnInfo {
    public final String name;
    public final RelDataType type;
    public final boolean nullable;

    public ColumnInfo(String name, RelDataType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    // TODO: I don't know how to manufacture a proper validator.
    private ColumnInfo(SqlColumnDeclaration decl, SqlValidator validator, boolean nullable) {
        this.name = Catalog.identifierToString(decl.name);
        this.type = decl.dataType.deriveType(validator);
        this.nullable = nullable;
    }
}
