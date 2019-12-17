package io.activedata.xnifi.exceptions;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * Created by MattU on 2018/1/24.
 */
public class BizException extends ProcessException {
    public BizException(String message) {
        super(message);
    }

    public BizException(Throwable cause) {
        super(cause);
    }

    public BizException(String message, Throwable cause) {
        super(message, cause);
    }
}
