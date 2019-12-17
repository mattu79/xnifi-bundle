package io.activedata.xnifi.neo4j;

import com.alibaba.fastjson.JSON;
import io.activedata.xnifi.utils.FlowFileUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.neo4j.driver.internal.value.EntityValueAdapter;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.SummaryCounters;

import java.util.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"neo4j", "graph", "network", "insert", "update", "delete", "put", "get", "node", "relationship", "connection", "executor"})
@CapabilityDescription("This processor executes a Neo4J Query (https://www.neo4j.com/) defined in the 'Neo4j Query' property of the "
        + "FlowFile and writes the result to the FlowFile body in JSON format. The processor has been tested with Neo4j version 3.4.5")
@WritesAttributes({
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.ERROR_MESSAGE, description = "Neo4J error message"),
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.LABELS_ADDED, description = "Number of labels added"),
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.NODES_CREATED, description = "Number of nodes created"),
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.NODES_DELETED, description = "Number of nodes deleted"),
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.PROPERTIES_SET, description = "Number of properties set"),
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.RELATIONS_CREATED, description = "Number of relationships created"),
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.RELATIONS_DELETED, description = "Number of relationships deleted"),
        @WritesAttribute(attribute = AbstractNeo4jCypherProcessor.ROWS_RETURNED, description = "Number of rows returned"),
})
public class Neo4jCypherOnJson extends AbstractNeo4jCypherProcessor {

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(tempRelationships);

        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(CONNECTION_URL);
        tempDescriptors.add(USERNAME);
        tempDescriptors.add(PASSWORD);
        tempDescriptors.add(QUERY);
        tempDescriptors.add(LOAD_BALANCING_STRATEGY);
        tempDescriptors.add(CONNECTION_TIMEOUT);
        tempDescriptors.add(MAX_CONNECTION_POOL_SIZE);
        tempDescriptors.add(MAX_CONNECTION_ACQUISITION_TIMEOUT);
        tempDescriptors.add(IDLE_TIME_BEFORE_CONNECTION_TEST);
        tempDescriptors.add(MAX_CONNECTION_LIFETIME);
        tempDescriptors.add(ENCRYPTION);
        tempDescriptors.add(TRUST_STRATEGY);
        tempDescriptors.add(TRUST_CUSTOM_CA_SIGNED_CERTIFICATES_FILE);

        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected void process(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
        Object params = FlowFileUtils.readJsonToObject(session, flowFile);

        try {
            long startTimeMillis = System.currentTimeMillis();

            StatementResult statementResult = executeQuery(query, params);
            List<Map<String, Object>> returnValue = statementResult.list(record -> {
                return record.asMap(v -> {
                    if (v instanceof EntityValueAdapter) {
                        return v.asMap();
                    } else {
                        return v.asObject();
                    }
                });
            });

            String json = JSON.toJSONString(returnValue);
            FlowFileUtils.write(session, flowFile, json);

            final long endTimeMillis = System.currentTimeMillis();

            if (getLogger().isDebugEnabled()) {
                getLogger().debug("执行查询[{}]花费时间{}ms，结果为：{}。", new Object[]{query, endTimeMillis, returnValue});
            }

            flowFile = populateAttributes(session, flowFile, statementResult, returnValue.size());

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, connectionUrl, (endTimeMillis - startTimeMillis));
        } catch (Exception exception) {
            getLogger().error("执行NEO4J操作失败：{}。",
                    new Object[]{exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, String.valueOf(exception.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected StatementResult executeQuery(String query, Object params) {
        try (Session session = getNeo4JDriver().session()) {
            if (params instanceof Map) {
                return session.run(query, (Map<String, Object>) params);
            } else if (params instanceof List) {
                HashMap paramMap = new HashMap();
                paramMap.put("jsonRows", params);
                return session.run(query, paramMap);
            } else {
                return session.run(query);
            }
        }
    }

    private FlowFile populateAttributes(final ProcessSession session, FlowFile flowFile,
                                        StatementResult statementResult, int size) {
        ResultSummary summary = statementResult.summary();
        SummaryCounters counters = summary.counters();

        Map<String, String> resultAttributes = new HashMap<>();
        resultAttributes.put(NODES_CREATED, String.valueOf(counters.nodesCreated()));
        resultAttributes.put(RELATIONS_CREATED, String.valueOf(counters.relationshipsCreated()));
        resultAttributes.put(LABELS_ADDED, String.valueOf(counters.labelsAdded()));
        resultAttributes.put(NODES_DELETED, String.valueOf(counters.nodesDeleted()));
        resultAttributes.put(RELATIONS_DELETED, String.valueOf(counters.relationshipsDeleted()));
        resultAttributes.put(PROPERTIES_SET, String.valueOf(counters.propertiesSet()));
        resultAttributes.put(ROWS_RETURNED, String.valueOf(size));

        flowFile = session.putAllAttributes(flowFile, resultAttributes);
        return flowFile;
    }
}
