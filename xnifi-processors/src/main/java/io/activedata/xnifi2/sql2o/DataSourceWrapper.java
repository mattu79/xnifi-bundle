package io.activedata.xnifi2.sql2o;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.dbcp.DBCPService;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * 用于包装单个连接为DataSource，以便sql2o使用
 */
public class DataSourceWrapper implements DataSource {
    DBCPService dbcpService;

    public DataSourceWrapper(DBCPService dbcpService) {
        Validate.notNull(dbcpService);
        this.dbcpService = dbcpService;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dbcpService.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return dbcpService.getConnection();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return DriverManager.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter printWriter) throws SQLException {
        DriverManager.setLogWriter(printWriter);
    }

    public void setLoginTimeout(int i) throws SQLException {
        DriverManager.setLoginTimeout(i);
    }

    public int getLoginTimeout() throws SQLException {
        return DriverManager.getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
