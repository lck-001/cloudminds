package app.ods;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import utils.KafkaUtil;
import utils.KafkaUtils;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class RocReadApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "roc_binlog_prod_new_3";
        String roc_t_user_library = "roc_t_library";
//        String roc_t_user_library = "roc_t_user_library";
        String groupId = "roc_binlog_prod_lck_test_01";

//        BucketAssigner<String, String> assigner = new DateTimeBucketAssigner<>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai"));
//        StreamingFileSink<String> hdfsSink = StreamingFileSink.<String>forRowFormat(
//                new Path("hdfs://nameservice1/tmp/rod/roc_test01"),
//                new SimpleStringEncoder<>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.DAYS.toMillis(1))//至少包含 20 分钟的数据
//                                .withInactivityInterval(TimeUnit.DAYS.toMillis(1))//最近 20 分钟没有收到新的数据
//                                .withMaxPartSize(1024 * 1024 * 1024)//文件大小已达到 1 GB
//                                .build())
//                .withBucketAssigner(assigner)
//                .build();

        DataStreamSource<String> source = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        DataStreamSource<String> roc_t_user_libraryDS = env.addSource(KafkaUtil.getKafkaConsumer(roc_t_user_library, groupId));
        roc_t_user_libraryDS.print("roc_t_user_library====");
        source.print("kafkasource===");
//        source
////                .map(line-> JSON.toJSONString(line))
//                .filter(line->
//                    "t_robot".equals(JSON.parseObject(line).getString("Table"))||
//                    "t_user_rcu_robot".equals(JSON.parseObject(line).getString("Table"))||
//                    "t_user_library".equals(JSON.parseObject(line).getString("Table"))||
//                    "t_library".equals(JSON.parseObject(line).getString("Table"))
//                )
//                .addSink(hdfsSink);
//                .print();
//                .writeAsText("hdfs://nameservice1/user/chengkang.li/test");

        // 将record-->过滤上传数据-->转换成jsonstring-->写入到hdfs
//        allDataDS.filter(log->log.event_type.equals("upload")).map(line-> JSON.toJSONString(line)).addSink(fileSink);
        env.execute();
    }
}
