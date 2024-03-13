package app.ods;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.KafkaUtils;

public class RocProduceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "roc_binlog_prod_new_3";
        String server = "10.11.33.58:9092";
        env.fromElements("test").addSink(KafkaUtils.getKafkaSink(topic,server));

//        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
//        source.addSink(KafkaUtils.getKafkaSink(topic,server));

        env.execute();
    }
}
