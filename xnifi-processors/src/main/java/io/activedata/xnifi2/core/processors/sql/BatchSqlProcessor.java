package io.activedata.xnifi2.core.processors.sql;

import io.activedata.xnifi.dbutils.utils.NamedParamSqlUtils;
import io.activedata.xnifi2.core.batch.AbstractBuilderSupportProcessor;
import io.activedata.xnifi2.core.batch.Input;
import io.activedata.xnifi2.core.batch.Output;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@TriggerSerially
@SupportsBatching
@Tags({"jdbc", "sql", "json", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("将数据记录（JSON/Avro）作为参数执行SQL语句")
public class BatchSqlProcessor extends AbstractBuilderSupportProcessor {

    private static final String KEY_RESULT = "result";
    private QueryRunner queryRunner = new QueryRunner();
    private MapListHandler mapListHandler = new MapListHandler();
    private MapHandler mapHandler = new MapHandler();
    private volatile Connection conn;
    private volatile String sqlMode;
    private volatile String originalSql;

    private static final String SQLMODE_QUERY_ONE = "SELECT_ONE";
    private static final String SQLMODE_QUERY = "SELECT/PROCEDURE";
    private static final String SQLMODE_UPDATE = "INSERT/UPDATE/DELETE";

    public static PropertyDescriptor PROP_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("executesql.dbcpservice")
            .displayName("数据库连接池服务")
            .description("用于存储序列参数的数据库连接池服务")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    public static PropertyDescriptor PROP_SQL_MODE = new PropertyDescriptor.Builder()
            .name("executesql.sqlmode")
            .displayName("SQL执行模式")
            .description("SQL执行模式，单条数据查询：SELECT_ONE/PRODURE_ONE；数据查询类：SELECT/PROCEDURE；数据操作类：INSERT/UPDATE/DELETE")
            .allowableValues(SQLMODE_QUERY_ONE, SQLMODE_QUERY, SQLMODE_UPDATE)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(SQLMODE_QUERY)
            .build();

    public static PropertyDescriptor PROP_SQL = new PropertyDescriptor.Builder()
            .name("executesql.sql")
            .displayName("待执行的SQL语句")
            .description("待执行的SQL语句，可使用\":field\"来引用记录的值。注意':'不能再引号中且前面必须有空格！")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<PropertyDescriptor>();
        props.add(PROP_DBCP_SERVICE);
        props.add(PROP_SQL_MODE);
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
        super.beforeProcess(context);

        sqlMode = context.getProperty(PROP_SQL_MODE).getValue();
        originalSql = context.getProperty(PROP_SQL).getValue();
        DBCPService dbcpService = context.getProperty(PROP_DBCP_SERVICE).asControllerService(DBCPService.class);
        conn = dbcpService.getConnection();
    }

    @Override
    protected void afterProcess() {
        DbUtils.closeQuietly(conn);
        super.afterProcess();
    }

    @Override
    protected Tuple<Relationship, Output> processRecordInternal(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception {
        Output output = excuteSqlWithMode(attributes, input);
        return new Tuple<>(REL_SUCCESS, output);
    }

    private Output excuteSqlWithMode(Map<String, String> attributes, Input input) throws SQLException {
        String[] paramNames = NamedParamSqlUtils.paramNames(originalSql);
        String sql = NamedParamSqlUtils.toNativeSql(originalSql);

        if (SQLMODE_UPDATE.equals(sqlMode)) {
            Output output = new Output();
            Integer count = 0;
            if (paramNames != null && paramNames.length > 0) {
                Object[] params = getNamedParams(paramNames, attributes, input);
                count = queryRunner.execute(conn, sql, params);
            } else {
                count = queryRunner.execute(conn, originalSql);
            }
            output.put(KEY_RESULT, count);
            return output;
        } else if (SQLMODE_QUERY_ONE.equals(sqlMode)) {
            if (paramNames != null && paramNames.length > 0) {
                Object[] params = getNamedParams(paramNames, attributes, input);
                Map<String, Object> result = queryRunner.query(conn, sql, mapHandler, params);
                return createQueryOneOutput(result);
            } else {
                Map<String, Object> result = queryRunner.query(conn, sql, mapHandler);
                return createQueryOneOutput(result);
            }
        } else {
            if (paramNames != null && paramNames.length > 0) {
                Object[] params = getNamedParams(paramNames, attributes, input);
                List<List<Map<String, Object>>> resultsList = queryRunner.execute(conn, sql, mapListHandler, params);
                return createQueryOutput(resultsList);
            } else {
                List<List<Map<String, Object>>> resultsList = queryRunner.execute(conn, sql, mapListHandler);
                return createQueryOutput(resultsList);
            }
        }
    }

    /**
     * 创建QUERY_ONE的输出对象
     * @param result
     * @return
     */
    private Output createQueryOneOutput(Map<String, Object> result){
        Output output = new Output();
        if (result != null)
            output.putAll(result);
        return output;
    }

    /**
     * 创建QUERY的输出对象
     *
     * @param resultsList 为对应多个结果集输出，所以这里resultsList是List<List<Map<String, Object>>>形式
     * @return
     */
    private Output createQueryOutput(List<List<Map<String, Object>>> resultsList) {
        Output output = new Output();
        if (resultsList != null) {
            if (resultsList.size() == 1) {
                // 处理单个结果集
                List<Map<String, Object>> results = resultsList.get(0);
                output.put(KEY_RESULT, results);
            } else if (resultsList.size() > 1) {
                // 处理多个结果集
                output.put(KEY_RESULT, resultsList);
            }
        }
        return output;
    }

    /**
     * 从JSON记录和属性中取得参数，优先从JSON记录中取，如果JSON记录中不存在则到属性中去取
     *
     * @param paramNames
     * @param attributes
     * @param input
     * @return
     */
    private static Object[] getNamedParams(String[] paramNames, Map<String, String> attributes, Input input) {
        List<Object> params = new ArrayList<>();
        for (String paramName : paramNames) {
            Object value = input.get(paramName);
            if (value == null)
                value = attributes.get(paramName);
            params.add(value);
        }
        return params.toArray();
    }
}
