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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.dbsp.type.*;
import org.dbsp.util.Unimplemented;

import java.util.ArrayList;
import java.util.List;

public class TypeCompiler {
    public TypeCompiler() {}

    public static Type makeZSet(Type elementType) {
        return new ZSetType(elementType.getNode(), elementType, CalciteToDBSPCompiler.weightType);
    }

    public Type convertType(RelDataType dt) {
        boolean nullable = dt.isNullable();
        if (dt.isStruct()) {
            List<Type> fields = new ArrayList<>();
            for (RelDataTypeField field: dt.getFieldList()) {
                Type type = this.convertType(field.getType());
                fields.add(type);
            }
            return new TTuple(dt, fields);
        } else {
            SqlTypeName tn = dt.getSqlTypeName();
            switch (tn) {
                case BOOLEAN:
                    return new TBool(tn, nullable);
                case TINYINT:
                    return new TSigned(tn, 8, nullable);
                case SMALLINT:
                    return new TSigned(tn, 16, nullable);
                case INTEGER:
                    return new TSigned(tn, 32, nullable);
                case BIGINT:
                case DECIMAL:
                    return new TSigned(tn, 64, nullable);
                case FLOAT:
                case REAL:
                    return new TFloat(tn, nullable);
                case DOUBLE:
                    return new TDouble(tn, nullable);
                case CHAR:
                case VARCHAR:
                    return new TString(tn, nullable);
                case BINARY:
                case VARBINARY:
                case NULL:
                case UNKNOWN:
                case ANY:
                case SYMBOL:
                case MULTISET:
                case ARRAY:
                case MAP:
                case DISTINCT:
                case STRUCTURED:
                case ROW:
                case OTHER:
                case CURSOR:
                case COLUMN_LIST:
                case DYNAMIC_STAR:
                case GEOMETRY:
                case SARG:
                case DATE:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    throw new Unimplemented(tn);
            }
        }
        throw new Unimplemented(dt);
    }
}
