package org.dbsp.util;

/**
 * Exception thrown for code that is not yet implemented.
 */
public class Unimplemented extends RuntimeException {
    public Unimplemented() {
        super("Not yet implemented");
    }

    public Unimplemented(Object obj) {
        super("Not yet implemented: " + obj.getClass().toString() + ":" + obj.toString());
    }

    public Unimplemented(String msg, boolean ignored) {
        super("Not yet implemented: " + msg);
    }
}
