package cloudminds.app;

import cloudminds.bean.ReportMetrics;
import cloudminds.conf.CephConfig;
import cloudminds.util.ClickHouseUtil;
import cloudminds.util.JsonUtil;
import cloudminds.util.KafkaUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static cloudminds.conf.CephConfig.*;

public class JsonParseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        String jsonStr = "{\"k8s_app_name\":\"roc\",\"json_extract\":{\"data\":{\"volume\":6,\"rcu\":{\"battery\":{\"temperature\":32,\"chargingStatus\":1,\"percent\":99}},\"robotStatus\":1,\"asrStatus\":1},\"serviceCode\":\"\",\"resultCode\":0,\"rcuId\":\"5013951A62CB\",\"source\":\"ROC\",\"robotId\":\"5013951A62CB\",\"resultInfo\":\"\",\"accountId\":\"AZ019179\",\"robotType\":\"patrol\",\"rodType\":\"reportMetrics\",\"tenantId\":\"patrol_jindi3\",\"guid\":\"9c20192006b845fd90714d5069b93d8b\",\"tag\":\"\",\"moduleId\":\"reportStatus\",\"timestamp\":\"2023-03-31T23:59:58.672+08:00\"},\"label\":\"bj-prod-221_roc_roc_roc-operation-service_harixaudit\",\"k8s_svc_name\":\"roc-operation-service\",\"k8s_env_name\":\"bj-prod-221\",\"output\":\"eskafka\"}";
//         env.fromElements(jsonStr)


        //设置动态参数
        ParameterTool propertiesargs = ParameterTool.fromArgs(args);
        String fileName = propertiesargs.get("CephConfPath");
        //从hdfs获取动态参数配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = FileSystem.get(URI.create(fileName), conf);
        fs.open(new org.apache.hadoop.fs.Path(fileName));
        ParameterTool propertiesFile = ParameterTool.fromPropertiesFile(fs.open(new org.apache.hadoop.fs.Path(fileName)).getWrappedStream());
        // 注册给环境变量(HBASE使用)
        env.getConfig().setGlobalJobParameters(propertiesFile);
        new CephConfig(propertiesFile);

        //2.设置CK&状态后端
        env.setStateBackend(new FsStateBackend(FSSTATEBACKEND));
        env.enableCheckpointing(100000);// 每 ** ms 开始一次 checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(100000);// Checkpoint 必须在** ms内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);// 确认 checkpoints 之间的时间会进行 ** ms
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//重启策略：重启3次，间隔10s

        OffsetsInitializer offsetsInitializer = null;
        if ("latest".equals(KAFKA_SOURCE_OFFSET)) {
            offsetsInitializer = OffsetsInitializer.latest();
        } else if ("earliest".equals(KAFKA_SOURCE_OFFSET)) {
            offsetsInitializer = OffsetsInitializer.earliest();
        } else {
            offsetsInitializer = OffsetsInitializer.timestamp(Long.parseLong(KAFKA_SOURCE_OFFSET));
        }
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SOURCE_SERVER)
                .setTopics(KAFKA_SOURCE_TOPIC)
                .setGroupId(KAFKA_SOURCE_GROUP)
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000")
//                .setStartingOffsets(OffsetsInitializer.latest())
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<String> filter = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (s.contains("74EE2ADB9394")) {
                    System.out.println(s);
                    return true;
                } else {
                    return false;
                }
            }
        });
        filter

                //3.从kafka中读取日志信息,将将每行数据转换为JavaBean对象 主流
//        env.addSource(KafkaUtils.getKafkaSource(KAFKA_SOURCE_TOPIC, KAFKA_SOURCE_GROUP))
                .flatMap(new RichFlatMapFunction<String, Object>() {
                    @Override
                    public void flatMap(String s, Collector<Object> collector) throws Exception {
                        ReportMetrics reportMetrics = new ReportMetrics();
                        JSONObject jsonStr = JSON.parseObject(s).getJSONObject("json_extract");
                        if (jsonStr != null && "reportMetrics".equals(jsonStr.getString("rodType"))) {
                            reportMetrics.event_time = jsonStr.getString("timestamp").substring(0, 23).replace("T", " ");
                            reportMetrics.tenant_id = jsonStr.getString("tenantId");
                            reportMetrics.robot_id = jsonStr.getString("robotId");
                            reportMetrics.rcu_id = jsonStr.getString("rcuId");
                            reportMetrics.user_id = jsonStr.getString("accountId");
                            reportMetrics.robot_type = jsonStr.getString("robotType");
                            reportMetrics.rod_type = jsonStr.getString("rodType");
                            reportMetrics.service_code = jsonStr.getString("serviceCode");
                            reportMetrics.k8s_env_name = "bj-prod-221";

                            JSONObject data = jsonStr.getJSONObject("data");
                            HashMap<String, Object> result_map = new HashMap<>();
                            Map<String, Object> map = JsonUtil.analysisJson(data.toString(), "", result_map);
                            Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, Object> next = iterator.next();
                                reportMetrics.attribute_name = next.getKey();
                                reportMetrics.attribute_value = next.getValue().toString();
                                collector.collect(reportMetrics);
                            }
                        }
                    }
                })
                .addSink(ClickHouseUtil.getJdbcSink("insert into " + CLICKHOUSE_TABLENAME +
                        "(event_time,tenant_id,robot_id,rcu_id,user_id,robot_type,rod_type,service_code,attribute_name,attribute_value,k8s_env_name) " +
                        " values (?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
