package io.activedata.xnifi.exceptions;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * 运行环境出现错误
 * Created by MattU on 2018/1/24.
 */
public class EnvironmentException extends ProcessException {
    public EnvironmentException(String message) {
        super(message);
    }

    public EnvironmentException(Throwable cause) {
        super(cause);
    }

    public EnvironmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
