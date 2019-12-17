package io.activedata.xnifi.processors.generate.dao.jdbc;

import io.activedata.xnifi.dbutils.handlers.BeanHandler;
import io.activedata.xnifi.processors.generate.dao.SequenceDAO;
import io.activedata.xnifi.processors.generate.sequence.Sequence;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by MattU on 2017/12/14.
 */
public class SequenceDAOImpl implements SequenceDAO {
    private static final String SQL_CREATE_TABLE = "create table XNIFI_GENERATED_SEQUENCE \n" +
            "(\n" +
            "   SEQ_CODE             VARCHAR(64)         not null,\n" +
            "   STRATEGY             VARCHAR(36),\n" +
            "   STEP                 VARCHAR(36),\n" +
            "   FORMAT               VARCHAR(64),\n" +
            "   INIT_VALUE           VARCHAR(64),\n" +
            "   MAX_VALUE            VARCHAR(64),\n" +
            "   VALUE                VARCHAR(64),\n" +
            "   PREV_VALUE           VARCHAR(64),\n" +
            "   LAST_UPDATE_TIME     DATE,\n" +
            "   constraint PK_XNIFI_GENERATED_SEQUENCE primary key (SEQ_CODE)\n" +
            ")";

    private static final String SQL_INSERT = "insert into XNIFI_GENERATED_SEQUENCE (seq_code,strategy,step,format,init_value,max_value,value, prev_value,last_update_time) values(?,?,?,?,?,?,?,?,?)";

    private static final String SQL_SELECT = "select seq_code as code,strategy,step,format,init_value,max_value,value,prev_value,last_update_time from XNIFI_GENERATED_SEQUENCE where seq_code = ?";

    private static final String SQL_DELETE = "delete from XNIFI_GENERATED_SEQUENCE where seq_code = ?";
    private QueryRunner runner = new QueryRunner(true);
    private Connection connection;

    public SequenceDAOImpl(Connection connection) {
        Validate.notNull(connection);
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.connection = connection;
    }

    public void createTable(){
        try {
            runner.execute(connection, SQL_CREATE_TABLE);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    @Override
    public void saveSequence(Sequence seq) {
        ScalarHandler handler = new ScalarHandler();
        try {
            runner.execute(connection, SQL_DELETE, seq.getCode());
            Object ret = runner.insert(connection, SQL_INSERT, handler,
                    seq.getCode(), seq.getStrategy(), seq.getStep(), seq.getFormat(), seq.getInitValue(),
                    seq.getMaxValue(), seq.getValue(), seq.getPrevValue(),seq.getLastUpdateTime());
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getStackTrace(e));
        }finally {
            commitQuietly(connection);
        }
    }

    @Override
    public Sequence getSequence(String seqCode) {
        ResultSetHandler<Sequence> handler = new BeanHandler<>(Sequence.class);
        try {
            Sequence sequence = runner.query(connection, SQL_SELECT, handler, seqCode);
            return sequence;
        } catch (SQLException e) {
            throw new RuntimeException(ExceptionUtils.getStackTrace(e));
        }finally {
            commitQuietly(connection);
        }
    }

    public static void commitQuietly(Connection conn){
        if (conn != null) {
            try {
                try {
                    conn.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } finally {
                //不关闭连接
            }
        }
    }
}
