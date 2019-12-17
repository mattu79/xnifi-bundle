package io.activedata.xnifi.processors.generate;


import com.alibaba.fastjson.JSON;
import io.activedata.xnifi.core.AbstractXNifiProcessor;
import io.activedata.xnifi.expression.Strings;
import io.activedata.xnifi.processors.generate.dao.SequenceDAO;
import io.activedata.xnifi.processors.generate.dao.jdbc.SequenceDAOImpl;
import io.activedata.xnifi.processors.generate.sequence.Sequence;
import io.activedata.xnifi.processors.generate.sequence.SequenceGenerator;
import io.activedata.xnifi.processors.generate.sequence.strategy.DateSequenceStrategy;
import io.activedata.xnifi.processors.generate.sequence.strategy.NumberSequenceStrategy;
import io.activedata.xnifi.utils.FlowFileUtils;
import io.activedata.xnifi.utils.SchemaUtils;
import io.activedata.xnifi.utils.ScriptContextUtils;
import jodd.bean.BeanCopy;
import org.apache.avro.file.CodecFactory;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.avro.WriteAvroResultWithSchema;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.mvel2.templates.TemplateRuntime;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.*;

/**
 * Created by MattU on 2017/12/13.
 */
@TriggerWhenEmpty
@Tags({ "sequence", "generate", "db" })
@CapabilityDescription("序列生成器")
public class GenerateSequence extends AbstractXNifiProcessor {

    private static AvroRecordSetWriter writerFactory = new AvroRecordSetWriter();

    private static SequenceGenerator generator = new SequenceGenerator();

    private volatile SequenceDAO dao;

    private volatile Connection conn;

    private static Map<String,Object> schemaMap = new HashMap<>();

    static{
        generator.registy(new NumberSequenceStrategy());
        generator.registy(new DateSequenceStrategy());
        Sequence sequence = new Sequence("code","strategy","step","yyyy-MM-dd","initValue","maxValue","value","prevValue",new Date());
        BeanCopy.beans(sequence,schemaMap).copy();
//        fromBean(sequence).toMap(schemaMap).copy();
    }


    public static PropertyDescriptor PROP_SEQ_CODE = new PropertyDescriptor.Builder()
            .name("generatesequence.seqcode")
            .displayName("序列代码")
            .description("用于确定唯一的序列代码")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor PROP_SEQ_STRATEGY = new PropertyDescriptor.Builder()
            .name("generatesequence.seqstrategy")
            .displayName("序列生成策略")
            .description("序列生成策略，可以是数字、日期")
            .defaultValue("SEQ_DATE")
            .allowableValues(new AllowableValue("SEQ_DATE", "时间"), new AllowableValue("SQL_NUMBER", "数字"))
            .required(true)
            .build();

    public static PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("generatesequence.batchsize")
            .displayName("批量生成数量")
            .description("批量生成数量，即也可以一次性生成多少个Sequence，一个Sequence对应一个FlowFile")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static PropertyDescriptor PROP_SEQ_STEP = new PropertyDescriptor.Builder()
            .name("generatesequence.seqstep")
            .displayName("序列步长")
            .description("序列步长可以是数字，也可以是数字加单位，如1D、1M、1Y分别代表一天、一个月、一年")
            .defaultValue("1D")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor PROP_SEQ_INIT_VALUE = new PropertyDescriptor.Builder()
            .name("generatesequence.seqinitvalue")
            .displayName("序列初始值")
            .description("序列初始值用于第一次生成序列的时候的初始值，可直接填写某个日期，格式使用序列值格式")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor PROP_SEQ_MAX_VALUE = new PropertyDescriptor.Builder()
            .name("generatesequence.seqmaxvalue")
            .displayName("序列最大值")
            .description("序列最大值一般使用函数形式，${$Dates.before(\"1D\")}代表调用日期函数before来取得前N天的时间，默认值是前一天")
            .defaultValue("${$Dates.before(\"1D\")}")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor PROP_SEQ_FORMAT = new PropertyDescriptor.Builder()
            .name("generatesequence.seqformat")
            .displayName("序列值的格式")
            .description("序列值的格式，在序列为日期策略时使用，这里使用Java的日期格式方式，yyyy达标年、MM代表月，dd代表天")
            .defaultValue("yyyy-MM-dd")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor PROP_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("generatesequence.dbcpservice")
            .displayName("数据库连接池服务")
            .description("用于存储序列参数的数据库连接池服务")
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<PropertyDescriptor>();
        props.add(PROP_SEQ_CODE);
        props.add(PROP_SEQ_STRATEGY);
        props.add(PROP_BATCH_SIZE);
        props.add(PROP_SEQ_STEP);
        props.add(PROP_SEQ_INIT_VALUE);
        props.add(PROP_SEQ_MAX_VALUE);
        props.add(PROP_SEQ_FORMAT);
        props.add(PROP_DBCP_SERVICE);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    protected void process(ProcessContext context, ProcessSession session) throws ProcessException {
        String seqStrategy = context.getProperty(PROP_SEQ_STRATEGY).getValue();
        String seqCode = context.getProperty(PROP_SEQ_CODE).getValue();
        String seqInitValue = context.getProperty(PROP_SEQ_INIT_VALUE).getValue();
        String seqStep = context.getProperty(PROP_SEQ_STEP).getValue();
        String seqFormat = context.getProperty(PROP_SEQ_FORMAT).getValue();
        String seqMaxValueExpr = context.getProperty(PROP_SEQ_MAX_VALUE).getValue(); // 这里的表达式可以为null，默认值会在evelMaxValue中产生
        String seqMaxValue = evalMaxValue(seqMaxValueExpr, seqFormat);
        int batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();

        Sequence seqParam = dao.getSequence(seqCode);
        if (seqParam == null){
            seqParam = new Sequence();
        }

        seqParam.setStrategy(seqStrategy);
        seqParam.setCode(seqCode);
        seqParam.setInitValue(seqInitValue);
        seqParam.setStep(seqStep);
        seqParam.setMaxValue(seqMaxValue);
        seqParam.setFormat(seqFormat);

        List<Sequence> seqs = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Sequence seq = generator.next(seqParam);
            if (seq == null) break; //如果seq为null，代表已经到达max value，所以退出循环

            seqParam = seq;
            seqs.add(seq);
        }

        RecordSchema schema = SchemaUtils.buildRecordSchema(schemaMap);
        for (Sequence seq : seqs){
            FlowFile flowFile = session.create();
            Map<String, String> attributes = createAttributes(context, flowFile);
            Map<String, Object> seqProps = new LinkedHashMap<>();
            BeanCopy.beans(seq,seqProps).copy();
            writeAttributes(session, flowFile, seqProps);
            FlowFileUtils.write(session, flowFile, JSON.toJSONString(seqProps));
            session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
        }

        if (seqs.size() > 0) {
            Sequence lastSeq = seqs.get(seqs.size() - 1);
            dao.saveSequence(lastSeq);
        }
    }

    private void writeAttributes(ProcessSession session, FlowFile flowFile, Map<String, Object> seqProps) {
        for (Map.Entry<String, Object> entry : seqProps.entrySet()){
            session.putAttribute(flowFile, entry.getKey(), Strings.toString(entry.getValue()));
        }
    }

    private void writeRecord(Map<String, String> attributes,ProcessSession session, FlowFile flowFile, Record record) {
        session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                RecordSetWriter writer = new WriteAvroResultWithSchema(AvroTypeUtil.extractAvroSchema(record.getSchema()), out, CodecFactory.nullCodec());
                writer.beginRecordSet();
                writer.write(record);
                WriteResult writeResult = writer.finishRecordSet();
                writer.flush();
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());
            }
        });
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        DBCPService dbcpService = context.getProperty(PROP_DBCP_SERVICE).asControllerService(DBCPService.class);
        conn = dbcpService.getConnection();
        dao = new SequenceDAOImpl(conn);
    }

    @Override
    protected void afterProcess() {
        DbUtils.closeQuietly(conn);
        dao = null;
    }

    @OnEnabled
    public void setContext(final ConfigurationContext context){
        writerFactory.storeSchemaAccessStrategy(context);
    }

    protected Map<String, String> createAttributes(ProcessContext context, FlowFile flowFile){
        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(flowFile.getAttributes());
        attributes.putAll(getDynamicProperties(context, flowFile)); //添加动态属性
        return attributes;
    }

    private Map<String, String> getDynamicProperties(ProcessContext context, FlowFile flowFile){
        Map<String, String> originalAttrs = flowFile.getAttributes();
        Map<String, String> dynamicProperties = new LinkedHashMap<>(context.getProperties().size());
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                String name = property.getName();
                String value = null;
                if (property.isExpressionLanguageSupported()){
                    try {
                        value = context.getProperty(property).evaluateAttributeExpressions(originalAttrs).getValue();
                    }catch (Exception e){
                        getLogger().warn("初始化属性时出现错误：", e);
                    }
                }else{
                    value = context.getProperty(property).getValue();
                }

                dynamicProperties.put(name, value);
            }
        }
        return dynamicProperties;
    }

    private static String evalMaxValue(String expr, String format){
        if (StringUtils.isBlank(format))
            format = "yyyy-MM-dd";

        if (StringUtils.isBlank(expr))
            expr = DateFormatUtils.format(new Date(), format); // 取得最大时间默认值

        Object result = TemplateRuntime.eval(expr, ScriptContextUtils.createContext()); // 这里没使用MVEL.eval是为了不把普通字符串识别成表达式
        if (result instanceof Date){
            return DateFormatUtils.format((Date) result, format);
        }else if (result instanceof String){
            return (String) result;
        }else{
            return Objects.toString(result, "");
        }
    }
}
