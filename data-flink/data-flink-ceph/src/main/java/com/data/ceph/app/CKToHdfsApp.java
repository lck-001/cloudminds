package com.data.ceph.app;

import com.data.ceph.config.CephConfig;
import com.data.ceph.utils.KafkaUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.hadoop.fs.FileSystem;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.net.URI;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import static com.data.ceph.config.CephConfig.*;

public class CKToHdfsApp {


    public static void main(String[] args) throws Exception {



        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("chengkang");
        props.setPassword("chengkang123");
        BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://10.11.33.163:8125/ceph_meta", props);
        ClickHouseConnection conn = dataSource.getConnection();
        ClickHouseStatement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select id_field from ceph_meta.mysql_type");
        while(rs.next()){
            int id_field = rs.getInt("id_field");
            System.out.println("id = "+id_field);
        }

    }
}
