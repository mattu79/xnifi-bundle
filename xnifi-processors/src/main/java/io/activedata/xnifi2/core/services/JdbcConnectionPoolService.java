package io.activedata.xnifi2.core.services;

import org.apache.nifi.dbcp.DBCPService;

import javax.sql.DataSource;

public interface JdbcConnectionPoolService extends DBCPService {
    DataSource getDataSource();
}
