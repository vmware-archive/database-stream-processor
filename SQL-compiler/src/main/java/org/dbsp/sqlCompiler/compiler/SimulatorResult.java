package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.sql.SqlNode;
import org.dbsp.util.ICastable;

/**
 * This class is a base class for the results produced by the DDL execution simulator.
 */
public interface SimulatorResult extends ICastable {
    SqlNode getNode();

    @Override
    default void error(String message) {
        throw new RuntimeException(message + ": " + this.getNode());
    }
}
