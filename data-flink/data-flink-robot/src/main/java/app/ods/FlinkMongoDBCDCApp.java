package app.ods;

import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkMongoDBCDCApp {
    public static void main(String[] args) throws Exception {
//1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DebeziumSourceFunction<String> mongoDBSource = MongoDBSource.<String>builder()
                .hosts("172.16.31.12:30310")
                .database("roc")
                .collection("reportAttribute_all")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.addSource(mongoDBSource).print();
        env.execute();
    }
}
