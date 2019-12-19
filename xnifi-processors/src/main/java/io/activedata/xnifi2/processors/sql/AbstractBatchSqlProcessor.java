package io.activedata.xnifi2.processors.sql;

import io.activedata.xnifi2.core.batch.AbstractBuilderSupportProcessor;
import io.activedata.xnifi2.core.batch.Output;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.sql2o.Sql2o;

import java.util.List;
import java.util.Map;

public abstract class AbstractBatchSqlProcessor extends AbstractBuilderSupportProcessor {

    private static final String KEY_RESULT = "result";

    protected volatile String originalSql;
    protected volatile DBCPService poolService;
    protected volatile Sql2o sql2o;

    public static PropertyDescriptor PROP_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("executesql.dbcpservice")
            .displayName("数据库连接池服务")
            .description("用于存储序列参数的数据库连接池服务")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();



    public static PropertyDescriptor PROP_SQL = new PropertyDescriptor.Builder()
            .name("executesql.sql")
            .displayName("待执行的SQL语句")
            .description("待执行的SQL语句，可使用\":field\"来引用记录的值。注意':'不能再引号中且前面必须有空格！")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();


    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        super.beforeProcess(context);

        originalSql = context.getProperty(PROP_SQL).getValue();
        poolService  = context.getProperty(PROP_DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    protected void afterProcess() {
        sql2o = null;
        super.afterProcess();
    }

    /**
     * 创建QUERY_ONE的输出对象
     * @param result
     * @return
     */
    protected Output createQueryOneOutput(Map<String, Object> result){
        Output output = new Output();
        if (result != null)
            output.putAll(result);
        return output;
    }

    /**
     * 创建QUERY的输出对象
     *
     * @param mapList 为对应多个结果集输出，所以这里resultsList是List<List<Map<String, Object>>>形式
     * @return
     */
    protected Output createQueryOutput(List<Map<String, Object>> mapList) {
        Output output = new Output();
        if (mapList != null) {
            output.put(KEY_RESULT, mapList);
        }
        return output;
    }
}
