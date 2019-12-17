package io.activedata.xnifi.core.batch;

import org.apache.nifi.processor.io.InputStreamCallback;

/**
 * 批处理回调函数
 * 该对象是概念上的对象定义，包装了InputStreamCallback，可以在该对象上自由包装处理逻辑
 * Created by MattU on 2018/1/27.
 */
public interface ProcessCallback extends InputStreamCallback {
    /**
     * 返回回调业务处理类
     * @return
     */
    CallbackHandler getHandler();
}
