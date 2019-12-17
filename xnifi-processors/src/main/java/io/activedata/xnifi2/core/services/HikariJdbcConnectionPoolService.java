package io.activedata.xnifi2.core.services;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 数据库连接池
 */
@Tags({"hikari", "jdbc", "database", "connection", "pooling"})
public class HikariJdbcConnectionPoolService extends AbstractControllerService implements JdbcConnectionPoolService {

    public static final PropertyDescriptor PROP_DRIVER_CLASS_NAME = new PropertyDescriptor.Builder()
            .name("hikaricp.driverclassname")
            .allowableValues(
                    new AllowableValue("com.mysql.jdbc.Driver", "MySQL"),
                    new AllowableValue("org.postgresql.Driver", "PostgreSQL"),
                    new AllowableValue("oracle.jdbc.driver.OracleDriver", "Oracle"),
                    new AllowableValue("com.microsoft.sqlserver.jdbc.SQLServerDriver", "SQLServer")
            )
            .defaultValue("com.mysql.jdbc.Driver")
            .displayName("数据库类型")
            .description("数据库类型，用于确定数据库驱动名")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_JDBC_URL = new PropertyDescriptor.Builder()
            .name("hikaricp.jdbcurl")
            .displayName("JDBC连接串")
            .description("JDBC连接串，MySQL连接串为jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8，Oracle连接串为jdbc:oracle:thin:@192.168.1.250:1521:devdb")
            .defaultValue("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name("hikaricp.username")
            .displayName("数据库用户名")
            .description("数据库用户名")
            .defaultValue("root")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name("hikaricp.password")
            .displayName("数据库密码")
            .description("数据库密码")
            .defaultValue("")
            .build();

    public static final PropertyDescriptor PROP_MIN_SIZE = new PropertyDescriptor.Builder()
        .name("hikaricp.minsize")
        .displayName("最小连接数")
        .description("最小连接数")
        .defaultValue("5")
        .build();

    public static final PropertyDescriptor PROP_MAX_SIZE = new PropertyDescriptor.Builder()
            .name("hikaricp.maxsize")
            .displayName("最大连接数")
            .description("最大连接数")
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor PROP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hikaricp.timeout")
            .displayName("连接超时时间，单位毫秒，默认10000毫秒（10秒）")
            .description("连接超时时间，单位毫秒，默认10000毫秒（10秒）")
            .defaultValue("10000")
            .build();


    private HikariDataSource hikariDataSource;

    @Override
    public DataSource getDataSource() {
        return hikariDataSource;
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return hikariDataSource.getConnection();
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        String jdbcUrl = context.getProperty(PROP_JDBC_URL).getValue();
        String driverClassName = context.getProperty(PROP_DRIVER_CLASS_NAME).getValue();
        String username = context.getProperty(PROP_USERNAME).getValue();
        String password = context.getProperty(PROP_PASSWORD).getValue();
        Integer minSize = context.getProperty(PROP_MIN_SIZE).asInteger();
        Integer maxSize = context.getProperty(PROP_MAX_SIZE).asInteger();
        Integer timeout = context.getProperty(PROP_TIMEOUT).asInteger();

        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setDriverClassName(driverClassName);
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMinimumIdle(minSize);
        hikariConfig.setMaximumPoolSize(maxSize);
        hikariConfig.setConnectionTimeout(timeout);

        hikariDataSource = new HikariDataSource(hikariConfig);
    }

    @OnDisabled
    public void shutdown() {
        try {
            hikariDataSource.close();
        } finally {
            hikariDataSource = null;
        }
    }
}
