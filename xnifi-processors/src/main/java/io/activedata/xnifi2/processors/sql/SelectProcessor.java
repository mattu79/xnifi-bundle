package io.activedata.xnifi2.processors.sql;

import io.activedata.xnifi2.core.batch.Input;
import io.activedata.xnifi2.core.batch.Output;
import io.activedata.xnifi2.support.sql2o.Sql2oHelper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;
import org.sql2o.Connection;
import org.sql2o.Query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SupportsBatching
@Tags({"jdbc", "sql", "select", "json", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("将数据记录（JSON/Avro）作为参数执行SQL语句")
public class SelectProcessor extends AbstractBatchSqlProcessor {


    private static final String SQLMODE_QUERY_ONE = "SELECT_ONE";
    private static final String SQLMODE_QUERY = "SELECT/PROCEDURE";
    private static final String SQLMODE_UPDATE = "INSERT/UPDATE/DELETE";

    protected volatile String sqlMode;

    public static PropertyDescriptor PROP_SQL_MODE = new PropertyDescriptor.Builder()
            .name("executesql.sqlmode")
            .displayName("SQL执行模式")
            .description("SQL执行模式，单条数据查询：SELECT_ONE/PRODURE_ONE；数据查询类：SELECT/PROCEDURE；数据操作类：INSERT/UPDATE/DELETE")
            .allowableValues(SQLMODE_QUERY_ONE, SQLMODE_QUERY, SQLMODE_UPDATE)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(SQLMODE_QUERY)
            .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<PropertyDescriptor>();
        props.add(PROP_DBCP_SERVICE);
        props.add(PROP_SQL);
        props.add(PROP_OUTPUT_RECORD_STRATEGY);
        props.add(PROP_INPUT_RECORD_TYPE);
        props.add(PROP_OUTPUT_RECORD_TYPE);
        props.add(PROP_OUTPUT_RECORD_EXAMPLE);
        props.add(PROP_RECORD_INPUT_BUILDER);
        props.add(PROP_RECORD_OUTPUT_BUILDER);
        return props;
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        sqlMode = context.getProperty(PROP_SQL_MODE).getValue();
        super.beforeProcess(context);
    }

    @Override
    protected Tuple<Relationship, Output> processRecordInternal(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception {
        Output output = excuteSqlWithMode(attributes, input);
        return new Tuple<>(REL_SUCCESS, output);
    }

    private Output excuteSqlWithMode(Map<String, String> attributes, Input input) throws SQLException {
        try (Connection con = sql2o.open()){
            Query query = con.createQuery(originalSql);
            query = Sql2oHelper.addAllParams(query, attributes, input);
            List<Map<String, Object>> results = query.executeAndFetchTable().asList();
            if (results != null && results.size() > 0) {
                Map<String, Object> result = results.get(0);
                return createQueryOneOutput(result);
            } else {
                return createQueryOutput(results);
            }
        }
    }
}
