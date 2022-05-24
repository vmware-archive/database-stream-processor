package org.dbsp.util;

/**
 * Exception thrown when an unsupported construct is compiled.
 */
public class UnsupportedException extends RuntimeException {
    public UnsupportedException(Object obj) {
        super("Not supported: " + obj.getClass().toString() + ":" + obj);
    }

    public UnsupportedException(String msg, boolean ignored) {
        super("Not supported: " + msg);
    }

    public UnsupportedException(String msg, Object obj) {
        super("Not supported: " + msg + obj.getClass().toString() + ":" + obj);
    }
}
