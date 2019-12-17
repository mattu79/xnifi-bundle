package io.activedata.xnifi.exceptions;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * 所有数据都处理错误的异常
 * Created by MattU on 2018/1/15.
 */
public class AllProcessFailedException extends ProcessException {
    public AllProcessFailedException(String message) {
        super(message);
    }

    public AllProcessFailedException(Throwable cause) {
        super(cause);
    }

    public AllProcessFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
