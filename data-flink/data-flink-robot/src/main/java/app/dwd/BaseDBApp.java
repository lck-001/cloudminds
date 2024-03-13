package app.dwd;

import app.function.CustomerDeserialization;
import app.function.DimSinkFunction;
import app.function.DimSinkFunction2;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.KafkaUtil;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.消费Kafka ods_base_db 主题数据创建流
//        String sourceTopic = "ods_base_db";
//        String groupId = "base_db_app_210325";
//        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        DebeziumSourceFunction<String> kafkaDS = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.user","test.student")
                .deserializer(new CustomerDeserialization())
                .serverTimeZone("UTC")
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = env.addSource(kafkaDS);

        //TODO 3.将每行数据转换为JSON对象并过滤(delete) 主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamSource.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        value.put("sinkTable","orderwide");
//                        value.put("sinkTable",value.getString("tableName"));
                        //取出数据的操作类型
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });

        //TODO 4.使用FlinkCDC消费配置表并处理成         广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("172.16.31.1")
                .port(31541)
                .username("bigdata_sync_r")
                .password("bigdata_sync_r")
                .databaseList("roc")
                .tableList("roc.t_user_rcu_robot")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);


        jsonObjDS.addSink(new DimSinkFunction2());

        env.execute();
    }
}
