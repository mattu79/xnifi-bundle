package io.activedata.xnifi2.core.utils;

import org.sql2o.Sql2o;

import javax.sql.DataSource;

public class Sql2oUtils {



    public static Sql2o create(DataSource dataSource){
        return new Sql2o(dataSource);
    }
}
