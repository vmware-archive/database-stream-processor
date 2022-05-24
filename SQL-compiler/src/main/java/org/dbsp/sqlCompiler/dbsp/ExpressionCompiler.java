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

import org.apache.calcite.rex.*;
import org.dbsp.sqlCompiler.dbsp.expression.*;
import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.Linq;
import org.dbsp.util.Unimplemented;

import java.util.List;

public class ExpressionCompiler extends RexVisitorImpl<Expression> {
    private final TypeCompiler typeCompiler = new TypeCompiler();
    public ExpressionCompiler(boolean deep) {
        super(deep);
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        Type type = this.typeCompiler.convertType(inputRef.getType());
        return new FieldExpression(inputRef, inputRef.getIndex(), type);
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
        Type type = this.typeCompiler.convertType(literal.getType());
        return new LiteralExpression(literal, type, literal.getValue().toString());
    }

    @Override
    public Expression visitCall(RexCall call) {
        List<Expression> ops = Linq.map(call.operands, e -> e.accept(this));
        Type type = this.typeCompiler.convertType(call.getType());
        switch (call.op.kind) {
            case TIMES:
                return new BinaryExpression(call, type, "*", ops.toArray(new Expression[2]));
            case DIVIDE:
                return new BinaryExpression(call, type, "/", ops.toArray(new Expression[2]));
            case MOD:
                return new BinaryExpression(call, type, "%", ops.toArray(new Expression[2]));
            case PLUS:
                return new BinaryExpression(call, type, "+", ops.toArray(new Expression[2]));
            case MINUS:
                return new BinaryExpression(call, type, "-", ops.toArray(new Expression[2]));
            case LESS_THAN:
                return new BinaryExpression(call, type, "<", ops.toArray(new Expression[2]));
            case GREATER_THAN:
                return new BinaryExpression(call, type, ">", ops.toArray(new Expression[2]));
            case LESS_THAN_OR_EQUAL:
                return new BinaryExpression(call, type, "<=", ops.toArray(new Expression[2]));
            case GREATER_THAN_OR_EQUAL:
                return new BinaryExpression(call, type, ">=", ops.toArray(new Expression[2]));
            case EQUALS:
                return new BinaryExpression(call, type, "==", ops.toArray(new Expression[2]));
            case NOT_EQUALS:
                return new BinaryExpression(call, type, "!=", ops.toArray(new Expression[2]));
            case OR:
                return new BinaryExpression(call, type, "||", ops.toArray(new Expression[2]));
            case AND:
                return new BinaryExpression(call, type, "&&", ops.toArray(new Expression[2]));
            case DOT:
                return new BinaryExpression(call, type, ".", ops.toArray(new Expression[2]));
            case NOT:
            case IS_FALSE:
            case IS_NOT_TRUE:
                return new UnaryExpression(call, type, "!", ops.toArray(new Expression[1]));
            case PLUS_PREFIX:
                return new UnaryExpression(call, type, "+", ops.toArray(new Expression[1]));
            case MINUS_PREFIX:
                return new UnaryExpression(call, type, "-", ops.toArray(new Expression[1]));
            case IS_TRUE:
            case IS_NOT_FALSE:
                assert ops.size() == 1 : "Expected 1 operand " + ops;
                return ops.get(0);
            case IS_NULL:
            case IS_NOT_NULL:
            case CAST:
            case FLOOR:
            case CEIL:
                throw new Unimplemented(call);
            case BIT_AND:
                return new BinaryExpression(call, type, "&", ops.toArray(new Expression[2]));
            case BIT_OR:
                return new BinaryExpression(call, type, "|", ops.toArray(new Expression[2]));
            case BIT_XOR:
                return new BinaryExpression(call, type, "^", ops.toArray(new Expression[2]));
            default:
                throw new Unimplemented(call);
        }
    }

    Expression compile(RexNode expression) {
        Expression compile = expression.accept(this);
        return new ClosureExpression(expression, compile.getType(), compile);
    }
}
