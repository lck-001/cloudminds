package app;

import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class MongoDBCDCApp {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.通过FlinkCDC构建SourceFunction
        SourceFunction<String> mongoDBSourceFunction = MongoDBSource.<String>builder()
                .hosts("172.16.31.1:31061")
//                .username("flinkuser")
//                .password("flinkpw")
                .database("rod")
                .collection("storage_file_video_2020")
//                .databaseList("penngo_db")
//                .collectionList("coll")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(mongoDBSourceFunction);


        SingleOutputStreamOperator<Object> singleOutputStreamOperator = dataStreamSource.process(new ProcessFunction<String, Object>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Object>.Context ctx, Collector<Object> out) {
                try {
                    System.out.println("processElement=====" + value);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

//        dataStreamSource.print("原始流--");
        env.execute("Mongo");
    }
}

