package io.activedata.xnifi.core;

import io.activedata.xnifi.exceptions.AllProcessFailedException;
import io.activedata.xnifi.exceptions.InvalidEnvironmentException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * xnifi平台最基础的NiFi处理器抽象类</br>
 * 抽象对异常的处理
 *
 * @author MattU
 */
public abstract class AbstractXNifiProcessor extends AbstractSessionFactoryProcessor {

    public String getName() {
        return getClass().getSimpleName();
    }

    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
            throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            beforeProcess(context);
            process(context, session);
            session.commit();
        } catch (final InvalidEnvironmentException | AllProcessFailedException e) {
            rollbackForException(context, session, e, true);
        } catch (final Exception e) {
            rollbackForException(context, session, e, true); //这里都进行回滚和惩罚，未使用Processor自己的回滚机制，因为NiFi自己取得的异常太简单
        } finally {
            afterProcess();
        }
    }

    protected void rollbackForException(ProcessContext context, ProcessSession session, Exception t, boolean doYield) {
        ComponentLog logger = getLogger();
        if (logger.isDebugEnabled()) {
            t.printStackTrace();
            getLogger().error("处理数据失败[{}]，将回滚会话操作。", new Object[]{ExceptionUtils.getStackTrace(t)});
        } else {
            t.printStackTrace();
            getLogger().error("处理数据失败[{}]，将回滚会话操作。", new Object[]{ExceptionUtils.getMessage(t)});
        }
        session.rollback(true); //回滚处理的FlowFile，这里惩罚没生效
        if (doYield) {
            context.yield(); //对整个Session进行延迟处理
        }
    }

    /**
     * 触发组件的处理逻辑，如果抛出任何异常，本次逻辑处理都会回滚
     *
     * @param context
     * @param session
     * @throws ProcessException
     */
    protected abstract void process(final ProcessContext context, final ProcessSession session)
            throws ProcessException;

    /**
     * 在执行处理逻辑之前对processor进行初始化，如果这里抛出异常，则整个处理过程将中断
     *
     * @param context
     * @throws ProcessException
     */
    protected void beforeProcess(ProcessContext context) throws ProcessException {
    }

    /**
     * 在处理逻辑执行完毕后调用
     */
    protected void afterProcess() {
    }
}
