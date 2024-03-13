package app;

import bean.VidioInfo;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import utils.ClickHouseUtil;
import utils.MongoDBSource;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class MongoDBExample {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        SingleOutputStreamOperator<VidioInfo> vidioDS = env.addSource(new MongoDBSource())
                .map(str -> {
                    VidioInfo vidioInfo = JSON.parseObject(str, VidioInfo.class);
                    vidioInfo.create_time = String.valueOf(Long.valueOf(vidioInfo.create_time) / 1000);
                    vidioInfo.update_time = String.valueOf(Long.valueOf(vidioInfo.update_time) / 1000);
                    return vidioInfo;
                });
        vidioDS.print();
//        vidioDS.addSink(ClickHouseUtil.getJdbcSink("insert into ceph_meta.dwd_video_info" +
//                        "(id,tenant_id,robot_id,robot_type,user_id,storage,business,file_id,file_dot,file_ext,file_pos,file_size,create_time,update_time,status,k8s_env_name)" +
//                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

//        BucketAssigner<String, String> assigner = new DateTimeBucketAssigner<>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai"));
//        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
//                new Path("hdfs://nameservice1/data/rodods/ods_rod_storage_file_video_i_d/"),
//                new SimpleStringEncoder<>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.DAYS.toMillis(1))//至少包含 20 分钟的数据
//                                .withInactivityInterval(TimeUnit.DAYS.toMillis(1 ))//最近 20 分钟没有收到新的数据
//                                .withMaxPartSize(1024 * 1024 * 1024)//文件大小已达到 1 GB
//                                .build())
//                .withBucketAssigner(assigner)
//                .build();

        // 将record-->过滤上传数据-->转换成jsonstring-->写入到hdfs
//        vidioDS.map(line->JSON.toJSONString(line)).addSink(fileSink);

        env.execute();

    }
}