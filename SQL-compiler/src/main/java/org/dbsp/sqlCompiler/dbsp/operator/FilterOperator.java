package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.expression.Expression;
import org.dbsp.sqlCompiler.dbsp.type.Type;

public class FilterOperator extends Operator {
    public FilterOperator(Object filter, Expression condition, Type type) {
        super(filter, "filter_keys", condition.toRustString(), TypeCompiler.makeZSet(type));
    }
}
