package io.activedata.xnifi.exceptions;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * 可重试的Exception
 * Created by MattU on 2018/1/29.
 */
public class RetrieableException extends ProcessException {
    public RetrieableException(String message) {
        super(message);
    }

    public RetrieableException(Throwable cause) {
        super(cause);
    }

    public RetrieableException(String message, Throwable cause) {
        super(message, cause);
    }
}
