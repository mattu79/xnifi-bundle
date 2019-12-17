/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.activedata.xnifi;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.activedata.xnifi2.core.services.JdbcConnectionPoolService;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Simple implementation only for GenerateTableFetch processor testing.
 */
public class DBCPServiceSimpleImpl extends AbstractControllerService implements JdbcConnectionPoolService {

    private DataSource dataSource;

    public DBCPServiceSimpleImpl() {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
        hikariConfig.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/xnifi_tests?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8");
        hikariConfig.setUsername("root");
        hikariConfig.setPassword("123456");
        hikariConfig.setMinimumIdle(5);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setConnectionTimeout(5000);

        dataSource = new HikariDataSource(hikariConfig);
    }

    @Override
    public String getIdentifier() {
        return "dbcp";
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }
}