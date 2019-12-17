package io.activedata.xnifi.processors.route;

/**
 * Created by MattU on 2017/11/28.
 */

import io.activedata.xnifi.core.AbstractXNifiProcessor;
import io.activedata.xnifi.utils.ContextUtils;
import io.activedata.xnifi.utils.FlowFileUtils;
import io.activedata.xnifi.utils.ScriptContextUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 抽象的路由数据记录的处理器
 * @param <T>
 */
public abstract class AbstractRouteJsonProcessor<T> extends AbstractXNifiProcessor {
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("在对数据记录进行路由过程中出现错误。")
            .build();

    public static final Relationship REL_UNMATCHED = new Relationship.Builder()
            .name("unmatched")
            .description("未匹配上任何表达式的记录。")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("每个FlowFile被路由后将被转移到该队列中。")
            .build();

    public static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("batch-size")
        .displayName("批处理大小")
        .description("每次读取多少个FlowFile进行处理")
        .addValidator(StandardValidators.NUMBER_VALIDATOR)
        .defaultValue("100")
        .build();

    protected Set<Relationship> routeRels = new HashSet<>();

    protected volatile int batchSize = 1;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_BATCH_SIZE);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        if (isRouteOriginal()) {
            relationships.add(REL_ORIGINAL);
        }

        relationships.add(REL_FAILURE);
        relationships.add(REL_UNMATCHED);
        relationships.addAll(routeRels);
        return relationships;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }

        final Relationship relationship = new Relationship.Builder()
                .name(descriptor.getName())
                .description("用户自定义的路由队列")
                .build();

        if (newValue == null) {
            routeRels.remove(relationship);
        } else {
            routeRels.add(relationship);
        }
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();
        super.beforeProcess(context);
    }

    @Override
    protected void process(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        for (FlowFile flowFile : flowFiles) {
            try {
                routeOnFlowFile(context, session, flowFile);
            }catch (Throwable e){
                ComponentLog logger = getLogger();
                if (logger.isDebugEnabled()) {
                    getLogger().error("路由JSON ROW时出现无法处理的错误[{}]。", new Object[]{ExceptionUtils.getStackTrace(e)});
                } else {
                    getLogger().error("路由JSON ROW出现无法处理的错误[{}]。", new Object[]{ExceptionUtils.getMessage(e)});
                }
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    /**
     * 对一个FlowFile上的JSON Row进行过滤
     * @param context
     * @param session
     * @param flowFile
     */
    private void routeOnFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        final T flowFileContext = getFlowFileContext(flowFile, context);
        //if (flowFileContext == null) return ;

        final AtomicInteger numRecords = new AtomicInteger(0);
        final Map<Relationship, Tuple<FlowFile, List<Map>>> writers = new HashMap<>();
        final FlowFile original = flowFile;
        final Map<String, String> attributes = createAttributes(context, flowFile);

        List<Map> rows = FlowFileUtils.readJsonToList(session, flowFile); //自定义方法

        if (rows != null){
            for (Map row : rows){
                numRecords.incrementAndGet();
                Map<String, Object> inputContext = ScriptContextUtils.createInputContext(row, attributes);
                Set<Relationship> rels = route(inputContext, flowFile, context, flowFileContext);

                for (final Relationship rel : rels) {
                    Tuple<FlowFile, List<Map>> tuple = writers.get(rel);
                    final List<Map> routedRows;
                    if (tuple == null) {
                        FlowFile outFlowFile = session.create(original);
                        routedRows = new ArrayList<Map>();
                        tuple = new Tuple<>(outFlowFile, routedRows);
                        writers.put(rel, tuple);
                    } else {
                        routedRows = tuple.getValue();
                    }

                    routedRows.add(row); //将过滤出来的行记录加入到对应的队列中
                }
            }

            for (Map.Entry<Relationship, Tuple<FlowFile, List<Map>>> entry : writers.entrySet()){
                Relationship rel = entry.getKey();
                Tuple<FlowFile, List<Map>> tuple = entry.getValue();
                FlowFile outFlowFile = tuple.getKey();
                List<Map> routedRows = tuple.getValue();

                if (outFlowFile != null && routedRows != null){
                    outFlowFile = FlowFileUtils.writeListToJson(session, outFlowFile, routedRows);
                }

                attributes.put("row.count", String.valueOf(routedRows.size()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

                FlowFile childFlowFile = session.putAllAttributes(outFlowFile, attributes);
                session.transfer(childFlowFile, rel);
                session.adjustCounter("Records Processed", routedRows.size(), false);
                session.adjustCounter("Records Routed to " +  rel.getName(), routedRows.size(), false);
                session.getProvenanceReporter().route(childFlowFile, rel);
            }

            if (isRouteOriginal()) {
                flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(numRecords));
                session.transfer(flowFile, REL_ORIGINAL);
            } else {
                session.remove(flowFile);
            }

            getLogger().debug("成功将{}条记录路由到{}个FlowFile文件中。", new Object[] {numRecords, writers.size()});
        }
    }

    /**
     * 创建新的FlowFile属性Map，里面包含原有属性值和动态属性值
     * @param context
     * @param flowFile
     * @return
     */
    protected Map<String, String> createAttributes(ProcessContext context, FlowFile flowFile){
        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(flowFile.getAttributes());
        attributes.putAll(FlowFileUtils.getDynamicProperties(context)); //添加动态属性
        return attributes;
    }

    /**
     *
     * @param inputContext
     * @param flowFile
     * @param context
     * @param flowFileContext
     * @return
     */
    protected abstract Set<Relationship> route(Map inputContext, FlowFile flowFile, ProcessContext context, T flowFileContext);

    protected abstract boolean isRouteOriginal();

    protected abstract T getFlowFileContext(FlowFile flowFile, ProcessContext context);
}

