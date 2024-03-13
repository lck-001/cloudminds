package com.cloudminds.kafka.jdbc.jdbc;

import com.cloudminds.kafka.jdbc.model.JdbcSinkProp;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcSinkUtil implements Serializable {


//    private static List columns = Arrays.asList("db","table","bigdata_method","event_time");
    public  SinkFunction<Row> getSink(String sql, JdbcSinkProp jdbcSinkProp){
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<Row>() {
//                    private String sqlTmp="insert into ${db}.${table}(${fields}) values(${place})";
                    @Override
                    public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
                        int arity = row.getArity();
                        for (int j = 0;j < arity;j++){
                            Object value = row.getField(j);
                            preparedStatement.setObject(j+1,value);
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(jdbcSinkProp.getBatchSize())
                        .withMaxRetries(jdbcSinkProp.getMaxRetries())
                        .withBatchIntervalMs(jdbcSinkProp.getBatchInterval())
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(jdbcSinkProp.getJdbcDriver())
                        .withUrl(jdbcSinkProp.getJdbcUrl())
                        .withUsername(jdbcSinkProp.getUsername())
                        .withPassword(jdbcSinkProp.getPassword())
                        .build());
    }
}
