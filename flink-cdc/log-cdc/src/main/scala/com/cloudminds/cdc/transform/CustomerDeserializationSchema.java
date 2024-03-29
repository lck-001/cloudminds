package com.cloudminds.cdc.transform;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;


import java.util.List;

public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 创建json对象用于封装结果数据
        JSONObject result = new JSONObject();

        // 获取库名 & 表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);

        // 获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();

        if (before != null) {
            // 获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);

        // 获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();

        if (after != null) {
            // 获取列信息
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op",operation);

        // 输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
