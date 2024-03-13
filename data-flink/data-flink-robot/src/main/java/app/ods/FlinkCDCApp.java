package app.ods;

import app.function.CustomerDeserialization;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCApp {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                localhost
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.t_user_rcu_robot")
                .deserializer(new CustomerDeserialization())
                .serverTimeZone("UTC")
                .startupOptions(StartupOptions.initial())
                .build();
                //roc_prod232
                //Caused by: org.apache.kafka.connect.errors.ConnectException: User does not have the 'LOCK TABLES' privilege required to obtain a consistent snapshot by preventing concurrent writes to tables.
//                .hostname("172.16.31.1")
//                .port(31541)
//                .username("bigdata_sync_r")
//                .password("bigdata_sync_r")
//                .databaseList("roc")
//                .tableList("roc.t_user_rcu_robot")
//                .deserializer(new CustomerDeserialization())
//                .startupOptions(StartupOptions.initial())
//                .build();
                //roc_prod251
                //Caused by: org.apache.kafka.connect.errors.ConnectException: User does not have the 'LOCK TABLES' privilege required to obtain a consistent snapshot by preventing concurrent writes to tables.
//                .hostname("172.16.31.28")
//                .port(30645)
//                .username("r_only_bigdata")
//                .password("r_only_bigdata")
//                .databaseList("roc")
//                .tableList("roc.t_user_rcu_robot")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new CustomerDeserialization())
//                .build();
                //boss_sit136
//                .hostname("172.16.23.4")
//                .port(30712)
//                .username("root")
//                .password("boss123456")
//                .databaseList("crss_upms")
//                .tableList("crss_upms.t_device")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new CustomerDeserialization())
//                .build();
                //boss_prod251
//                .hostname("172.16.31.28")
//                .port(31083)
//                .username("r_big_data")
//                .password("1234QWER")
//                .databaseList("crss_upms")
//                .tableList("crss_upms.tenant")
//                .startupOptions(StartupOptions.initial())
//                .serverTimeZone("UTC")
//                .deserializer(new CustomerDeserialization())
//                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3.打印数据并将数据写入Kafka
        streamSource.print("flink cdc ====");
//        String sinkTopic = "ods_base_db";
//        streamSource.addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        //4.启动任务
        env.execute("FlinkCDC");
    }
}
