package org.apache.livy.exception;

public class LivyException extends Exception {
	private static final long serialVersionUID = 1L;

	public LivyException() {
    }

    public LivyException(String message) {
        super(message);
    }

    public LivyException(String message, Throwable cause) {
        super(message, cause);
    }

    public LivyException(Throwable cause) {
        super(cause);
    }

    public LivyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
