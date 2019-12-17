package io.activedata.xnifi.core.batch;

import io.activedata.xnifi.core.*;
import io.activedata.xnifi.exceptions.InvalidEnvironmentException;
import io.activedata.xnifi.utils.RecordConverter;
import io.activedata.xnifi.utils.ContextUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by MattU on 2018/1/24.
 */
public class AsyncRecordsProcessCallback implements ProcessCallback {
    public static final PropertyDescriptor PROP_RECORD_READER = new PropertyDescriptor.Builder()
            .name("record.reader")
            .displayName("输入记录读取器")
            .description("请指定用于读取输入数据的RecordReader服务。")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record.writer")
            .displayName("输出记录写入器")
            .description("请指定用于输出数据的RecordWriter服务。")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_MAX_ERROR_COUNT = new PropertyDescriptor.Builder()
            .name("max.error.count")
            .displayName("最大错误数")
            .description("每批数据处理的可以容忍的最大错误数量")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PROCESS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("processRecord.timeout")
            .displayName("处理超时时间（单位：毫秒）")
            .description("每个任务的处理超时时间（单位：毫秒），默认5000毫秒")
            .required(true)
            .defaultValue("5000")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    private LinkedHashMap<CompletableFuture<Tuple<Relationship, Record>>, Record> futureMap = new LinkedHashMap<>();
    private Map<Relationship, Tuple<FlowFile, RecordSetWriter>> writers = new HashMap<>();
    private AtomicInteger recordTotal = new AtomicInteger(0);
    private AtomicInteger recordError = new AtomicInteger(0);

    private ComponentLog logger;
    private ProcessContext context;
    private ProcessSession session;
    private FlowFile original;
    private RecordSchema inputSchema;
    private RecordSchema outputSchema;
    private Map<String, String> attributes;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private CallbackHandler handler;
    private long processTimeout;
    private int maxErrorCount;

    public AsyncRecordsProcessCallback(ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session, CallbackHandler handler) {
        Validate.notNull(logger, "参数logger不能为null。");
        Validate.notNull(original, "参数original不能为null。");
        Validate.notNull(context, "参数context不能为null。");
        Validate.notNull(session, "参数session不能为null。");
        Validate.notNull(handler, "参数processHandler不能为null。");

        this.logger = logger;
        this.original = original;
        this.context = context;
        this.session = session;
        this.handler = handler;
        this.readerFactory = context.getProperty(PROP_RECORD_READER).asControllerService(RecordReaderFactory.class);
        this.writerFactory = context.getProperty(PROP_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        this.attributes = ContextUtils.createAttributes(context, original);
        this.processTimeout = context.getProperty(PROP_PROCESS_TIMEOUT).asLong();
        this.maxErrorCount = context.getProperty(PROP_MAX_ERROR_COUNT).asInteger();
    }

    @Override
    public CallbackHandler getHandler() {
        return handler;
    }

    @Override
    public void process(InputStream in) throws IOException {
        try (final RecordReader reader = readerFactory.createRecordReader(attributes, in, logger)) {
            inputSchema = reader.getSchema();
            //inputSchema = writerFactory.getSchema(attributes, reader.getSchema());
            outputSchema = buildOutputSchema(inputSchema);
            Validate.notNull(inputSchema, "inputSchema不能为null。");
            Validate.notNull(outputSchema, "outputSchema不能为null。");

            Record record;
            while ((record = reader.nextRecord()) != null) {
                final Record recordData = record;
                CompletableFuture future = CompletableFuture
                        .supplyAsync(() -> processRecord(recordData, original, context));  //recordData原始数据
//                        .exceptionally((e) -> {
//                            logger.warn("执行请求时出现错误："+ ExceptionUtils.getStackTrace(e));
//                            return new Tuple<>(AbstractBatchProcessor.REL_FAILURE, recordData);
//                            //FIXME 应该放到Failure队列中
//                        });
                futureMap.put(future, recordData);
            }
            futureMap.keySet().stream().map(this::waitForCompleted).count();
            closeWriters();
        } catch (final Exception e) {
            throw new InvalidEnvironmentException("处理输入流时出现问题：" + ExceptionUtils.getStackTrace(e));
        }
    }

    protected Tuple<Relationship, Record> waitForCompleted(CompletableFuture<Tuple<Relationship, Record>> future) {
        Tuple<Relationship, Record> result = null;
        try {
            result = future.get(processTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {  //超时异常和中断异常
            logger.warn("等待数据处理完成时出现错误，该数据将被重新处理：" + ExceptionUtils.getStackTrace(e));
            future.completeExceptionally(e);

            int error = recordError.incrementAndGet();
            if (error >= maxErrorCount) {
                //throw new EnvironmentException("");
            } else {
                //logger.error("AAAAAAAAAAAAAA", e);
                //throw new BizException(e);
            }
            Record originalRecord = futureMap.get(future);
            result = new Tuple<>(AbstractBatchProcessor.REL_RETRY, originalRecord);
        } catch (ExecutionException e) {  //线程异常
            //logger.error("数据在处理过程中出现错误：", e);
            future.completeExceptionally(e);
            Record originalRecord = futureMap.get(future);
            originalRecord.setValue("errorMessage",e.getMessage());
            result = new Tuple<>(AbstractBatchProcessor.REL_FAILURE, originalRecord);
        }

        try {
            writeRecord(result.getValue(), result.getKey());
            recordTotal.incrementAndGet();
            return result;
        } catch (SchemaNotFoundException | IOException e) {
            throw new InvalidEnvironmentException(e);
        }
    }

    Tuple<Relationship, Record> processRecord(Record record, FlowFile flowFile, ProcessContext context) {
        Map<String, String> attributes = ContextUtils.createAttributes(context, flowFile);
        Map<String, Object> recordData = RecordConverter.convertToMap(record);
        Input input = new Input(recordData);
        Tuple<Relationship, Output> result = null;

        result = getHandler().handleProcess(attributes, input, flowFile, context);

        Relationship rel = result.getKey();
        Output output = result.getValue();
        RecordSchema schema = inputSchema; //如果输出是普通输出，则使用已有writeSchema

        if (AbstractBatchProcessor.REL_FAILURE.equals(rel)) {
            schema = FailureOutput.FAILURE_OUTPUT_SCHEMA;
        }else if (AbstractBatchProcessor.REL_RETRY.equals(rel)){
            schema = inputSchema;
        }else{
            schema = outputSchema;
        }
        Validate.notNull(schema, "schema不能为null。");

        Record outputRecord = new MapRecord(schema, result.getValue());
        return new Tuple<>(rel, outputRecord);
    }

    protected void writeRecords(Record record, Set<Relationship> relationships) throws IOException, SchemaNotFoundException {
        for (final Relationship relationship : relationships) {
            writeRecord(record, relationship);
        }
    }

    protected void writeRecord(Record record, Relationship relationship) throws SchemaNotFoundException, IOException {
        final RecordSetWriter recordSetWriter;
        Tuple<FlowFile, RecordSetWriter> tuple = writers.get(relationship);
        if (tuple == null) {
            FlowFile outFlowFile = session.create(original);
            final OutputStream out = session.write(outFlowFile);
            RecordSchema schema = record.getSchema();
            Validate.notNull(schema, record.toString());
            recordSetWriter = writerFactory.createWriter(logger, schema, out);
            recordSetWriter.beginRecordSet();
            tuple = new Tuple<>(outFlowFile, recordSetWriter);
            writers.put(relationship, tuple);
        } else {
            recordSetWriter = tuple.getValue();
        }
        recordSetWriter.write(record);

        //TODO 可嵌入日志收集机制
    }

    protected void closeWriters() {
        for (final Map.Entry<Relationship, Tuple<FlowFile, RecordSetWriter>> entry : writers.entrySet()) {
            final Relationship relationship = entry.getKey();
            final Tuple<FlowFile, RecordSetWriter> tuple = entry.getValue();
            final RecordSetWriter writer = tuple.getValue();
            FlowFile childFlowFile = tuple.getKey();

            WriteResult writeResult = closeWriter(writer);
            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
            attributes.putAll(writeResult.getAttributes());

            childFlowFile = session.putAllAttributes(childFlowFile, attributes);
            session.transfer(childFlowFile, relationship);
            session.adjustCounter("Records Processed", writeResult.getRecordCount(), false);
            session.adjustCounter("Records Routed to " + relationship.getName(), writeResult.getRecordCount(), false);

            session.getProvenanceReporter().route(childFlowFile, relationship);
        }
    }



    /**
     * 利用输入schema构造输出schema
     * @param inputSchema
     * @return
     */
    private RecordSchema buildOutputSchema(RecordSchema inputSchema){
        Validate.notNull(inputSchema, "inputSchema不能为null。");

        List<RecordField> resultFields = handler.getResultFields();
        if (resultFields == null || resultFields.size() <= 0) {
            return inputSchema;
        }else{
            List<RecordField> outputFields = new ArrayList<>();
            outputFields.addAll(resultFields);
            outputFields.addAll(inputSchema.getFields());

            RecordSchema recordSchema = new SimpleRecordSchema(outputFields);
            validateSchema(recordSchema);
            return recordSchema;
        }
    }

    /**
     * FIXME 迁移到统一的工具方法中
     * @param recordSchema
     */
    private void validateSchema(RecordSchema recordSchema) {
        try {
            AvroTypeUtil.extractAvroSchema(recordSchema);
        }catch (Exception e){
            throw new InvalidEnvironmentException("RecordSchema不可用，存在以下问题：" + ExceptionUtils.getStackTrace(e));
        }
    }

    private WriteResult closeWriter(RecordSetWriter writer) throws ProcessException {
        try {
            final WriteResult writeResult = writer.finishRecordSet();
            return writeResult;
        } catch (final IOException ioe) {
            logger.warn("结束数据写入失败：{}。", new Object[]{ExceptionUtils.getStackTrace(ioe)});
            throw new InvalidEnvironmentException("结束数据写入失败：", ioe);
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                logger.warn("关闭数据写入器失败：{}。", new Object[]{ExceptionUtils.getStackTrace(e)});
                e.printStackTrace();
            }
        }
    }
}
