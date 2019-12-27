package io.activedata.xnifi2.core.batch;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import io.activedata.xnifi.core.AbstractXNifiProcessor;
import io.activedata.xnifi2.core.batch.callback.AsyncInputStreamCallback;
import io.activedata.xnifi2.core.batch.callback.SyncInputStreamCallback;
import io.activedata.xnifi2.core.batch.reader.AvroRecordReaderFactory;
import io.activedata.xnifi2.core.batch.reader.JsonRecordReaderFactory;
import io.activedata.xnifi2.core.batch.writer.AvroRecordWriterFactory;
import io.activedata.xnifi2.core.batch.writer.JsonRecordWriterFactory;
import io.activedata.xnifi2.core.validators.JsonValidator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.Tuple;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 抽象的批处理处理器，引入了ProcessCallback机制；同时为了便于Callback逻辑的实现，该Processor实现了CallbackHandler接口
 *
 * @author MattU
 */
public abstract class AbstractBatchProcessor extends AbstractXNifiProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("成功处理的FlowFile队列。")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("处理过程出现错误的FlowFile队列。")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("可尝试重新处理的FlowFile队列。")
            .build();

    public static final String WSS_BOTH_INPUT_AND_OUTPUT = "BOTH_INPUT_AND_OUTPUT";
    public static final String WSS_ONLY_OUTPUT = "ONLY_OUTPUT";

    public static final String RECORD_TYPE_AVRO = "AVRO";
    public static final String RECORD_TYPE_JSON = "JSON";

    public static final PropertyDescriptor PROP_INPUT_RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("input.record.type")
            .displayName("输入记录类型")
            .description("输入记录类型，JSON或AVRO格式。")
            .defaultValue(RECORD_TYPE_JSON)
            .allowableValues(new AllowableValue(RECORD_TYPE_JSON, "JSON格式"), new AllowableValue(RECORD_TYPE_AVRO, "AVRO格式"))
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_OUTPUT_RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("output.record.type")
            .displayName("输出记录类型")
            .description("输出记录类型，JSON或AVRO格式。")
            .defaultValue(RECORD_TYPE_JSON)
            .allowableValues(new AllowableValue(RECORD_TYPE_JSON, "JSON格式"), new AllowableValue(RECORD_TYPE_AVRO, "AVRO格式"))
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_OUTPUT_RECORD_STRATEGY = new PropertyDescriptor.Builder()
            .name("output.record.strategy")
            .displayName("输出生成策略")
            .description("请选择输出的生成策略，包括只输出Output、Input和Output都输出两种策略。")
            .defaultValue(WSS_BOTH_INPUT_AND_OUTPUT)
            .allowableValues(new AllowableValue(WSS_BOTH_INPUT_AND_OUTPUT, "输入和输出"), new AllowableValue(WSS_ONLY_OUTPUT, "只有输出"))
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_OUTPUT_RECORD_EXAMPLE = new PropertyDescriptor.Builder()
            .name("output.record.example")
            .displayName("输出记录示例，JSON格式。")
            .description("输出记录示例，用实际数据来制定输出记录的格式。")
            .addValidator(JsonValidator.INSTANCE)
            .build();

    public static final PropertyDescriptor PROP_IS_ASYNC_MODE = new PropertyDescriptor.Builder()
            .name("is.async.mode")
            .displayName("是否使用异步模式")
            .description("是否使用异步模式")
            .defaultValue("false")
            .allowableValues(new AllowableValue("true", "是"), new AllowableValue("false", "否"))
            .build();

    protected volatile boolean isAsyncMode = false;
    protected volatile String inputRecordType;
    protected volatile String outputRecordType;
    protected volatile String outputRecordExample;
    protected volatile String outputRecordStrategy;

    @Override
    protected final void process(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            InputStreamCallback callback = createCallback(this, getLogger(), flowFile, context, session);
            session.read(flowFile, callback);
            session.remove(flowFile);
        } catch (ProcessException e) {
            throw e;
        } catch (Throwable e) {
            throw new ProcessException("处理FlowFile时出现未知错误：" + ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * 创建一个能够处理输入的回调
     *
     * @param processor
     * @param logger
     * @param original
     * @param context
     * @param session
     * @return
     */
    protected InputStreamCallback createCallback(AbstractBatchProcessor processor, ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session) {
        RecordReaderFactory readerFactory = null;
        RecordSetWriterFactory writerFactory = null;

        if (RECORD_TYPE_AVRO.equals(inputRecordType)) {
            readerFactory = new AvroRecordReaderFactory();
        } else if (RECORD_TYPE_JSON.equals(inputRecordType)) {
            readerFactory = new JsonRecordReaderFactory();
        }

        boolean mergeSchema = WSS_BOTH_INPUT_AND_OUTPUT.equals(outputRecordStrategy);
        if (RECORD_TYPE_AVRO.equals(outputRecordType)) {
            RecordSchema outputSchema = getOutputSchema();
            writerFactory = new AvroRecordWriterFactory(outputSchema, mergeSchema);
        } else if (RECORD_TYPE_JSON.equals(outputRecordType)) {
            RecordSchema outputSchema = getOutputSchema();
            writerFactory = new JsonRecordWriterFactory(outputSchema, mergeSchema);
        }

        if (isAsyncMode){
            return new AsyncInputStreamCallback(this, readerFactory, writerFactory, logger, original, context, session, outputRecordStrategy);
        } else {
            return new SyncInputStreamCallback(this, readerFactory, writerFactory, logger, original, context, session, outputRecordStrategy);
        }
    }

    public Tuple<Relationship, Output> processRecord(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception {
        Tuple<Relationship, Output> result = processRecordInternal(attributes, input, original, context);
        return result;
    }

    /**
     * 对每条记录进行处理，如果处理过程中出现错误可抛出异常，后续会根据异常类型对记录进行分别处理
     * BizException：该记录会被路由到错误的队列中
     * RetrieableException：该记录会被路由到重试队列中
     * 其他Exception：往整个处理器抛出异常，可能导致处理器停止
     *
     * @param attributes
     * @param input
     * @param original
     * @param context
     * @return
     * @throws ProcessException
     */
    protected abstract Tuple<Relationship, Output> processRecordInternal(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception;

    /**
     * 默认通过OutputRecordExample来取得Output的RecordSchema，可继承改写
     * @return
     */
    protected RecordSchema getOutputSchema(){
        Map recordExample = JSON.parseObject(outputRecordExample, Map.class);
        return DataTypeUtils.inferSchema(recordExample, null, Charsets.UTF_8);
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        isAsyncMode = context.getProperty(PROP_IS_ASYNC_MODE).asBoolean();
        inputRecordType = context.getProperty(PROP_INPUT_RECORD_TYPE).getValue();
        outputRecordType = context.getProperty(PROP_OUTPUT_RECORD_TYPE).getValue();
        outputRecordExample = context.getProperty(PROP_OUTPUT_RECORD_EXAMPLE).getValue();
        outputRecordStrategy = context.getProperty(PROP_OUTPUT_RECORD_STRATEGY).getValue();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_RETRY);
        return relationships;
    }
}
