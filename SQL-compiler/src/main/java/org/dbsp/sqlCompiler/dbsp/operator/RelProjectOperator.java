package org.dbsp.sqlCompiler.dbsp.operator;

import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.type.Type;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Relational projection operator.  Projects a tuple on a set of columns.
 */
public class RelProjectOperator extends Operator {
    static String projectFunction(List<Integer> projectIndexes) {
        return "|t| (" + String.join(", ", Linq.map(projectIndexes, i -> "t." + i)) + ")";
    }

    public RelProjectOperator(@Nullable Object node, List<Integer> projectIndexes,
                              Type elementType) {
        super(node, "map_keys", projectFunction(projectIndexes), TypeCompiler.makeZSet(elementType));
    }
}
