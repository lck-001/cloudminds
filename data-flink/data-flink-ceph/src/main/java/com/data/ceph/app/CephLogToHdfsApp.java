package com.data.ceph.app;

import com.alibaba.fastjson.JSON;
import com.data.ceph.bean.CephAccessLog;
import com.data.ceph.bean.CephAccessRecord;
import com.data.ceph.bean.CephNginxLog;
import com.data.ceph.config.CephConfig;
import com.data.ceph.function.DimAsyncFunction;
import com.data.ceph.utils.*;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.data.ceph.config.CephConfig.*;

public class CephLogToHdfsApp {


    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
        env.enableCheckpointing(10000);// 每 ** ms 开始一次 checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(100000);// Checkpoint 必须在** ms内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);// 确认 checkpoints 之间的时间会进行 ** ms
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10,TimeUnit.SECONDS)));//重启策略：重启3次，间隔10s

        //3.从kafka中读取日志信息
        DataStreamSource<String> dataStream = env.addSource(KafkaUtils.getKafkaSource(KAFKA_SOURCE_TOPIC, KAFKA_SOURCE_GROUP));



        //4.将原始数据写入hdfs
        BucketAssigner<String, String> assigner = new DateTimeBucketAssigner<>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai"));
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
                new Path(HDFS_FILE_PATH),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.DAYS.toMillis(1))//至少包含 20 分钟的数据
                                .withInactivityInterval(TimeUnit.DAYS.toMillis(1 ))//最近 20 分钟没有收到新的数据
                                .withMaxPartSize(1024 * 1024 * 1024)//文件大小已达到 1 GB
                                .build())
                .withBucketAssigner(assigner)
                .build();

        dataStream.addSink(fileSink);

        //10.流环境执行
        env.execute();
    }
}
