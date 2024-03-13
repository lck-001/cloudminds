package app;

import bean.NatsData;
import bean.UeLocation;
import com.alibaba.fastjson.JSON;
import conf.NatsConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.hadoop.fs.FileSystem;
import utils.ClickHouseUtil;
import utils.NatsUtil;

import java.io.*;
import java.net.URI;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static conf.NatsConfig.*;

public class NatsApp {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置动态参数
        ParameterTool propertiesargs = ParameterTool.fromArgs(args);
        String fileName = propertiesargs.get("NatsConfPath");
        //从hdfs获取动态参数配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = FileSystem.get(URI.create(fileName), conf);
        fs.open(new org.apache.hadoop.fs.Path(fileName));
        ParameterTool propertiesFile = ParameterTool.fromPropertiesFile(fs.open(new org.apache.hadoop.fs.Path(fileName)).getWrappedStream());

//         ParameterTool parameters = null;
//        try {
//            Properties props = new Properties();
//            InputStream inputStream = new FileInputStream(fileName);
//            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
//            props.load(bf);
//            parameters = ParameterTool.fromMap((Map) props);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // 注册给环境变量(HBASE使用)
        env.getConfig().setGlobalJobParameters(propertiesFile);
        new NatsConfig(propertiesFile);

        //2.设置CK&状态后端
//        env.setStateBackend(new FsStateBackend(FSSTATEBACKEND));
        env.enableCheckpointing(10000);// 每 ** ms 开始一次 checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(100000);// Checkpoint 必须在** ms内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);// 确认 checkpoints 之间的时间会进行 ** ms
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//重启策略：重启3次，间隔10s

        SingleOutputStreamOperator<UeLocation> ueLocationData = env.addSource(new NatsUtil())
                .map(new MapFunction<String, UeLocation>() {
                    @Override
                    public UeLocation map(String str) throws Exception {
                        NatsData natsData = JSON.parseObject(str, NatsData.class);
                        UeLocation ueLocation = new UeLocation();
                        System.out.println("natsdata :" + natsData.toString());
                        ueLocation.guid = natsData.guid;
                        ueLocation.robot_id = natsData.robotid;
                        ueLocation.tenant_id = natsData.tenantid;
                        ueLocation.map_id = natsData.mapid;
                        ueLocation.mapping3d_x = natsData.mapping3d_x;
                        ueLocation.mapping3d_y = natsData.mapping3d_y;
                        ueLocation.mapping3d_z = natsData.mapping3d_z;
                        ueLocation.mapping3d_angle = natsData.mapping3d_angle;
                        ueLocation.ue_loc_x = natsData.ue_loc_x;
                        ueLocation.ue_loc_y = natsData.ue_loc_y;
                        ueLocation.ue_loc_z = natsData.ue_loc_z;
                        ueLocation.ue_rotate_pitch = natsData.ue_rotate_pitch;
                        ueLocation.ue_rotate_roll = natsData.ue_rotate_roll;
                        ueLocation.ue_rotate_yaw = natsData.ue_rotate_yaw;
                        ueLocation.event_time = natsData.timestamp;
                        ueLocation.k8s_env_name = "bj-test-86";
                        System.out.println("uedata:" + ueLocation.guid);
                        return ueLocation;
                    }
                });
        ueLocationData
                .addSink(ClickHouseUtil.<UeLocation>getJdbcSink("insert into " + CLICKHOUSE_TABLENAME +
                        "(guid,robot_id,tenant_id,map_id,mapping3d_x,mapping3d_y,mapping3d_z,mapping3d_angle,ue_loc_x,ue_loc_y,ue_loc_z,ue_rotate_pitch,ue_rotate_roll,ue_rotate_yaw,event_time,k8s_env_name)" +
                        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        BucketAssigner<String, String> assigner = new DateTimeBucketAssigner<>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai"));
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
                new Path(HDFS_FILE_PATH),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.DAYS.toMillis(1))//至少包含 20 分钟的数据
                                .withInactivityInterval(TimeUnit.DAYS.toMillis(1))//最近 20 分钟没有收到新的数据
                                .withMaxPartSize(1024 * 1024 * 1024)//文件大小已达到 1 GB
                                .build())
                .withBucketAssigner(assigner)
                .build();

        // 将数据-->转换成jsonstring-->写入到hdfs
        ueLocationData.map(line -> JSON.toJSONString(line)).addSink(fileSink);


        env.execute();
    }
}
