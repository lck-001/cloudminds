package com.cloudminds.cdc.jdbc;

import com.cloudminds.cdc.model.JdbcSinkProp;
import com.cloudminds.cdc.model.sink.JdbcSinkModel;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class JdbcSinkUtil implements Serializable {

    private final LinkedHashMap<String, DataType> schemaMap;
    private final JdbcSinkModel jdbcSinkModel;
    private final String protocol;
    public JdbcSinkUtil(LinkedHashMap<String, DataType> schema, JdbcSinkModel jdbcSinkModel){
        this.schemaMap = schema;
        this.jdbcSinkModel = jdbcSinkModel;
        this.protocol = jdbcSinkModel.jdbcUrl().split(":")[1];
    }

    private Object getClickHouseDataValue(Object value,DataType dataType) {
        Object o = null;
        if (dataType.equals(DataTypes.STRING())) {
            o = value == null ? "" : value;
        }else if (dataType.equals(DataTypes.BIGINT())){
            o = value == null ? 0 : value;
        }else if (dataType.equals(DataTypes.INT())){
            o = value == null ? 0 : value;
        }else if (dataType.equals(DataTypes.DATE()) || dataType.equals(DataTypes.TIMESTAMP()) || dataType.equals(DataTypes.TIMESTAMP(3)) || dataType.equals(DataTypes.TIMESTAMP(6))){
            o = value == null ? 0 : value;
        }else if (dataType.equals(DataTypes.ARRAY(DataTypes.STRING()))){
            List<Object> list = new ArrayList<>();
            o = value == null ? list : value;
        }else if (dataType.equals(DataTypes.FLOAT())){
            o = value == null ? 0 : value;
        }else if (dataType.equals(DataTypes.DOUBLE())){
            o = value == null ? 0 : value;
        }else{
            o = value == null ? "" : value;
        }
        return o;
    }

//    private static List columns = Arrays.asList("db","table","bigdata_method","event_time");
    public  SinkFunction<Row> getSink(String sql){
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<Row>() {
//                    private String sqlTmp="insert into ${db}.${table}(${fields}) values(${place})";
                    @Override
                    public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
/*                        int i = 0;
//                        HashMap<String, Object> placeMap = new HashMap<>();
//                        ArrayList<String> fields = new ArrayList<>();
//                        ArrayList<String> place = new ArrayList<>();
                        Set<String> fieldNames = row.getFieldNames(false);
                        assert fieldNames != null;
                        for (String fieldName : fieldNames){
                            i+=1;
                            Object value = row.getField(fieldName);
                            preparedStatement.setObject(i,value);

//                            fields.add(fieldName);
//                            place.add("?");
                        }*/
                        int i = 0;
                        assert schemaMap != null;
                        for (Map.Entry<String,DataType> entry : schemaMap.entrySet()){
                            String fieldName = entry.getKey();
                            if (!ExcludeColumns.excludeColumnsMap.containsKey(fieldName)){
                                Object value = row.getField(fieldName);
                                if ("clickhouse".equalsIgnoreCase(protocol)){
                                    value = getClickHouseDataValue(value, entry.getValue());
                                }
                                i+=1;
                                preparedStatement.setObject(i,value);
                            }
                        }
//                        String fieldStr = String.join(",", fields);
//                        String placeStr = String.join(",", place);
//                        placeMap.put("fields",fieldStr);
//                        placeMap.put("place",placeStr);
//                        String sql = Placeholder.replace(sqlTmp, placeMap, "${", "}", false);
//                        preparedStatement.execute(sql);
                       /* int arity = row.getArity();
                        for (int j = 0;j < arity;j++){
                            Object value = row.getField(j);
                            if (value instanceof String){
                                System.out.println("true");
                            }else if (value instanceof Integer){
                                System.out.println("ture");
                            }
                            preparedStatement.setObject(j+1,value);
                        }*/
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(jdbcSinkModel.batchSize())
                        .withMaxRetries(jdbcSinkModel.maxRetries())
                        .withBatchIntervalMs(jdbcSinkModel.batchInterval())
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(jdbcSinkModel.jdbcDriver())
                        .withUrl(jdbcSinkModel.jdbcUrl())
                        .withUsername(jdbcSinkModel.username())
                        .withPassword(jdbcSinkModel.password())
                        .build());
    }
}
