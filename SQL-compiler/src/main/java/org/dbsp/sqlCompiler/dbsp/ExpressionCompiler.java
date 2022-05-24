package org.dbsp.sqlCompiler.dbsp;

import org.apache.calcite.rex.*;
import org.dbsp.sqlCompiler.dbsp.expression.Expression;
import org.dbsp.sqlCompiler.dbsp.expression.FieldExpression;
import org.dbsp.sqlCompiler.dbsp.type.Type;

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

    Expression compile(RexNode expression) {
        return expression.accept(this);
    }
}
