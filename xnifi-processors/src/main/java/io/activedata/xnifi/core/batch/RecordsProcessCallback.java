package io.activedata.xnifi.core.batch;

import io.activedata.xnifi.core.FailureOutput;
import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import io.activedata.xnifi.exceptions.InvalidEnvironmentException;
import io.activedata.xnifi.utils.RecordConverter;
import io.activedata.xnifi.utils.ContextUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by MattU on 2018/1/24.
 */
public class RecordsProcessCallback implements ProcessCallback {
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

    private Map<Relationship, Tuple<FlowFile, RecordSetWriter>> writers = new HashMap<>();
    private AtomicInteger recordTotal = new AtomicInteger();
    private AtomicInteger recordError = new AtomicInteger();

    private ComponentLog logger;
    private ProcessContext context;
    private ProcessSession session;
    private FlowFile original;
    private Map<String, String> attributes;
    private RecordSchema writeSchema;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private CallbackHandler handler;

    public RecordsProcessCallback(ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session, CallbackHandler handler) {
        Validate.notNull(logger, "参数logger不能为null。");
        Validate.notNull(original, "参数original不能为null。");
        Validate.notNull(context, "参数context不能为null。");
        Validate.notNull(session, "参数session不能为null。");

        this.logger = logger;
        this.original = original;
        this.context = context;
        this.session = session;
        this.handler = handler;
        this.readerFactory = context.getProperty(PROP_RECORD_READER).asControllerService(RecordReaderFactory.class);
        this.writerFactory = context.getProperty(PROP_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        this.attributes = ContextUtils.createAttributes(context, original);
    }

    @Override
    public CallbackHandler getHandler() {
        return handler;
    }

    @Override
    public void process(InputStream in) throws IOException {
        try (final RecordReader reader = readerFactory.createRecordReader(attributes, in, -1, logger)) {
            final RecordSchema writeSchema = writerFactory.getSchema(attributes, reader.getSchema());
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    Tuple<Relationship, Record> result = processRecord(record, writeSchema, original, context);
                    writeRecord(result.getValue(), result.getKey());
                }
                closeWriters();
        } catch (final Exception e) {
            throw new InvalidEnvironmentException("处理输入流时出现问题：" + ExceptionUtils.getStackTrace(e));
        }
    }

    Tuple<Relationship, Record> processRecord(Record record, RecordSchema writeSchema, FlowFile flowFile, ProcessContext context){
        Map<String, String> attributes = ContextUtils.createAttributes(context, flowFile);
        Map<String, Object> recordData = RecordConverter.convertToMap(record);
        Input input = new Input(recordData);
        Tuple<Relationship, Output> result = getHandler().handleProcess(attributes, input, flowFile, context);
        Relationship rel = result.getKey();
        Output output = result.getValue();
        RecordSchema schema = writeSchema; //如果输出是普通输出，则使用已有writeSchema
        if (output instanceof FailureOutput){
            schema = FailureOutput.FAILURE_OUTPUT_SCHEMA;
        }
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
            recordSetWriter = writerFactory.createWriter(logger, writeSchema, out);
            recordSetWriter.beginRecordSet();

            tuple = new Tuple<>(outFlowFile, recordSetWriter);
            writers.put(relationship, tuple);
        } else {
            recordSetWriter = tuple.getValue();
        }
        recordSetWriter.write(record);
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

    private WriteResult closeWriter(RecordSetWriter writer) throws ProcessException {
        try {
            final WriteResult writeResult = writer.finishRecordSet();
            return writeResult;
        } catch (final IOException ioe) {
            logger.warn("结束数据写入失败：{}。", new Object[]{ExceptionUtils.getStackTrace(ioe)});
            throw new InvalidEnvironmentException("结束数据写入失败：", ioe);
        }finally {
            try {
                writer.close();
            } catch (IOException e) {
                logger.warn("关闭数据写入器失败：{}。", new Object[]{ExceptionUtils.getStackTrace(e)});
                e.printStackTrace();
            }
        }
    }
}
