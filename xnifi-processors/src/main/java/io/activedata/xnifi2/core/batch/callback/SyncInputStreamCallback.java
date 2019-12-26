package io.activedata.xnifi2.core.batch.callback;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import io.activedata.xnifi.exceptions.InvalidEnvironmentException;
import io.activedata.xnifi.exceptions.RetrieableException;
import io.activedata.xnifi.expression.Dates;
import io.activedata.xnifi.utils.ContextUtils;
import io.activedata.xnifi2.core.batch.AbstractBatchProcessor;
import io.activedata.xnifi2.core.batch.Input;
import io.activedata.xnifi2.core.batch.Output;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class SyncInputStreamCallback implements InputStreamCallback {
    private static final String FIELD_ERROR_MESSAGE = "errorMessage";
    private static final String FIELD_AT_TIME = "atTime";

    private AtomicInteger recordTotal = new AtomicInteger();
    private AtomicInteger recordError = new AtomicInteger();
    private Map<Relationship, Tuple<FlowFile, RecordSetWriter>> writers = new HashMap<>();


    private AbstractBatchProcessor processor;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private ComponentLog logger;
    private ProcessContext context;
    private ProcessSession session;
    private FlowFile original;
    private RecordSchema inputSchema;
    private RecordSchema errorSchema;
    private RecordSchema outputSchema;
    private Map<String, String> attributes;
    private String writerSchemaStrategy;

    public SyncInputStreamCallback(AbstractBatchProcessor processor, RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory, ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session, String writerSchemaStrategy) {
        Validate.notNull(processor, "参数processor不能为null。");
        Validate.notNull(readerFactory, "参数readerFactory不能为null。");
        Validate.notNull(writerFactory, "参数writerFactory不能为null。");
        Validate.notNull(logger, "参数logger不能为null。");
        Validate.notNull(original, "参数original不能为null。");
        Validate.notNull(context, "参数context不能为null。");
        Validate.notNull(session, "参数session不能为null。");
        Validate.notBlank(writerSchemaStrategy, "参数writerSchemaStrategy不能为空。");

        this.processor = processor;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
        this.original = original;
        this.context = context;
        this.session = session;
        this.attributes = ContextUtils.createAttributes(context, original);
        this.writerSchemaStrategy = writerSchemaStrategy;
    }

    @Override
    public void process(InputStream in) throws IOException {
        try (final RecordReader reader = readerFactory.createRecordReader(attributes, in, -1, logger)) {
            initErrorSchema(reader.getSchema());

            try {
                this.inputSchema = reader.getSchema();
                this.outputSchema = writerFactory.getSchema(attributes, reader.getSchema());
            } catch (Exception e) {
                logger.debug("WriterFactory无法提供OutputSchema。");
            } // 根据WriterFactory初始化OutputSchema，如果WriterFactory无法提供则使用第一条输出记录来计算OutputSchema
            Record record;
            while ((record = reader.nextRecord()) != null) {
                processAndWriteRecord(record, original, context);
            }
            closeWriters();
        } catch (final Exception e) {
            e.printStackTrace();
            throw new InvalidEnvironmentException("处理输入流时出现问题：" + ExceptionUtils.getStackTrace(e));
        }
    }

    protected void processAndWriteRecord(Record inputRecord, FlowFile flowFile, ProcessContext context) throws Exception {
        try {
            Tuple<Relationship, Record> result = processRecord(attributes, inputRecord, original, context);
            writeRecord(outputSchema, result.getValue(), result.getKey());
        } catch (RetrieableException e) {   //自定义错误,可以重新运行的错误
            String errorMessage = ExceptionUtils.getStackTrace(e);
            logger.debug("数据处理错误，写入重试队列：" + errorMessage + "。");
            Map<String, Object> inputData = new LinkedHashMap<>(inputRecord.toMap());
            inputData.put(FIELD_ERROR_MESSAGE, errorMessage);
            inputData.put(FIELD_AT_TIME, Dates.timestamp());
            MapRecord retryRecord = new MapRecord(errorSchema, inputData);
            writeRecord(errorSchema, retryRecord, processor.REL_RETRY);
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = ExceptionUtils.getStackTrace(e);
            logger.debug("数据处理错误，写入失败队列：" + errorMessage + "。");
            Map<String, Object> inputData = new LinkedHashMap<>(inputRecord.toMap());
            inputData.put(FIELD_ERROR_MESSAGE, errorMessage);
            inputData.put(FIELD_AT_TIME, Dates.timestamp());
            MapRecord failureRecord = new MapRecord(errorSchema, inputData);
            writeRecord(errorSchema, failureRecord, processor.REL_FAILURE);
        }
    }

    protected Tuple<Relationship, Record> processRecord(Map<String, String> attributes, Record inputRecord, FlowFile flowFile, ProcessContext context) throws Exception {
        Input input = new Input(inputRecord);
        Tuple<Relationship, Output> result = processor.processRecord(attributes, input, flowFile, context);
        initSchemas(input, result.getValue());
        if (processor.WSS_BOTH_INPUT_AND_OUTPUT.equals(writerSchemaStrategy)) {
            Output inheritResult = new Output();
            inheritResult.putAll(input);
            inheritResult.putAll(result.getValue());
            inheritResult.put(FIELD_AT_TIME, Dates.timestamp());
            MapRecord outputRecord = new MapRecord(this.outputSchema, inheritResult);
            return new Tuple<>(result.getKey(), outputRecord);
        } else {
            result.getValue().put(FIELD_AT_TIME, Dates.timestamp());
            MapRecord outputRecord = new MapRecord(this.outputSchema, result.getValue());
            return new Tuple<>(result.getKey(), outputRecord);
        }
    }

    protected void initSchemas(Input input, Output output) {
        if (this.outputSchema == null) {
            synchronized (this) {
                if (this.inputSchema == null) {
                    RecordSchema inputRecordSchema = DataTypeUtils.inferSchema(input, null, Charsets.UTF_8);
                    this.inputSchema = inputRecordSchema;
                }

                if (this.outputSchema == null) {
                    RecordSchema outputRecordSchema = DataTypeUtils.inferSchema(output, null, Charsets.UTF_8);
                    if (processor.WSS_BOTH_INPUT_AND_OUTPUT.equals(writerSchemaStrategy)) {
                        this.outputSchema = DataTypeUtils.merge(this.inputSchema, outputRecordSchema);
                    } else {
                        this.outputSchema = outputRecordSchema;
                    }
                }
            }
        }
    }

    protected void initErrorSchema(RecordSchema recordSchema) {
        if (this.errorSchema == null) {
            synchronized (this) {
                if (this.errorSchema == null) {
                    List<RecordField> fields = recordSchema.getFields();
                    List<RecordField> inputFields = Lists.newLinkedList(fields);
                    inputFields.add(new RecordField(FIELD_ERROR_MESSAGE, RecordFieldType.STRING.getDataType()));
                    inputFields.add(new RecordField(FIELD_AT_TIME, RecordFieldType.TIMESTAMP.getDataType()));
                    this.errorSchema = new SimpleRecordSchema(inputFields);
                }
            }
        }
    }

    protected void writeRecord(RecordSchema outputSchema, Record outputRecord, Relationship relationship) throws SchemaNotFoundException, IOException {
        final RecordSetWriter recordSetWriter;
        Tuple<FlowFile, RecordSetWriter> tuple = writers.get(relationship);
        if (tuple == null) {
            FlowFile outFlowFile = session.create(original);
            final OutputStream out = session.write(outFlowFile);
            recordSetWriter = writerFactory.createWriter(logger, outputSchema, out);
            recordSetWriter.beginRecordSet();
            tuple = new Tuple<>(outFlowFile, recordSetWriter);
            writers.put(relationship, tuple);
        } else {
            recordSetWriter = tuple.getValue();
        }
        recordSetWriter.write(outputRecord);
    }

    private void closeWriters() {
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
            System.err.println("==============================" + childFlowFile);
            session.transfer(childFlowFile, relationship);
            session.adjustCounter("Records Processed", writeResult.getRecordCount(), false);
            session.adjustCounter("Records Routed to " + relationship.getName(), writeResult.getRecordCount(), false);
            session.getProvenanceReporter().route(childFlowFile, relationship);
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
