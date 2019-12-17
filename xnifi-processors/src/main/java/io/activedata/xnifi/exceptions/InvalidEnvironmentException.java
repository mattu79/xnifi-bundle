package io.activedata.xnifi.exceptions;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * 不可用的运行环境异常（包括磁盘、网络等运行环境不可用）
 *
 * Created by MattU on 2018/1/19.
 */
public class InvalidEnvironmentException extends ProcessException {
    public InvalidEnvironmentException(String message) {
        super(message);
    }

    public InvalidEnvironmentException(Throwable cause) {
        super(cause);
    }

    public InvalidEnvironmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
