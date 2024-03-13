package cloudminds.app;

import cloudminds.bean.ReportAttribute;
import cloudminds.bean.ReportMetrics;
import cloudminds.conf.CephConfig;
import cloudminds.util.ClickHouseUtil;
import cloudminds.util.JsonUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static cloudminds.conf.CephConfig.*;

public class ReportAttributeApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置动态参数
        ParameterTool propertiesargs = ParameterTool.fromArgs(args);
        String fileName = propertiesargs.get("CephConfPath");

        //从本地文件获取动态参数配置文件
        ParameterTool propertiesFile = null;
        try {
            Properties props = new Properties();
            InputStream inputStream = new FileInputStream(fileName);
            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
            props.load(bf);
            propertiesFile = ParameterTool.fromMap((Map) props);
        } catch (IOException e) {
            e.printStackTrace();
        }


//        //从hdfs获取动态参数配置文件
//        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//        FileSystem fs = FileSystem.get(URI.create(fileName), conf);
//        fs.open(new org.apache.hadoop.fs.Path(fileName));
//        ParameterTool propertiesFile = ParameterTool.fromPropertiesFile(fs.open(new org.apache.hadoop.fs.Path(fileName)).getWrappedStream());
        // 注册给环境变量(HBASE使用)
        env.getConfig().setGlobalJobParameters(propertiesFile);
        new CephConfig(propertiesFile);

        //2.设置CK&状态后端
//        env.setStateBackend(new FsStateBackend(FSSTATEBACKEND));
        env.enableCheckpointing(100000);// 每 ** ms 开始一次 checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(100000);// Checkpoint 必须在** ms内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);// 确认 checkpoints 之间的时间会进行 ** ms
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//重启策略：重启3次，间隔10s


        DebeziumSourceFunction<String> roc232 = MongoDBSource.<String>builder()
                .hosts("172.16.31.12:30310")
                .databaseList("roc")
                .collectionList("roc.reportAttribute_all")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        env.addSource(roc232)
                .flatMap(new FlatMapFunction<String, Object>() {
                    @Override
                    public void flatMap(String s, Collector<Object> collector) throws Exception {

                        ReportAttribute reportAttribute = new ReportAttribute();
                        JSONObject jsonStr = JSON.parseObject(s).getJSONObject("fullDocument");
                        if (jsonStr != null) {
                            reportAttribute.rcu_id = jsonStr.getString("_id");
                            reportAttribute.k8s_env_name = "bj-prod-232";

                            JSONObject data = jsonStr.getJSONObject("reportAttribute");
                            reportAttribute.event_time = data.getJSONObject("timestamp").getLong("$numberLong");
                            HashMap<String, Object> result_map = new HashMap<>();
                            Map<String, Object> map = JsonUtil.analysisJson(data.toString(), "", result_map);
                            Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, Object> next = iterator.next();
                                reportAttribute.attribute_name = next.getKey();
                                reportAttribute.attribute_value = next.getValue().toString();

                                if ("timestamp__D_numberLong".equals(reportAttribute.attribute_name)) continue;
                                collector.collect(reportAttribute);
                            }
                        }
                    }
                })
                .addSink(ClickHouseUtil.getJdbcSink("insert into " + CLICKHOUSE_TABLENAME +
                        "(event_time,rcu_id,attribute_name,attribute_value,k8s_env_name) " +
                        " values (?,?,?,?,?)"));


        env.execute();
    }
}
