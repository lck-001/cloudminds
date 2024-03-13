package com.data.ceph.app;

import com.data.ceph.bean.CephAccessRecord;
import com.data.ceph.bean.UserInfo;
import com.data.ceph.function.DimAsyncFunction;
import com.data.ceph.utils.ClickHouseUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


public class CephWithUserApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("C:\\workspace\\data-flink\\data-flink-ceph\\data\\access_key.txt");

//        source.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                if (s.length() >= 20) {
//                    System.out.println(s.substring(1, 20));
//                } else {
//                    System.out.println("异常数据：" + s);
//                }
//                return null;
//            }
//        });

        SingleOutputStreamOperator<CephAccessRecord> validDS = source
                .map(line -> {
                    CephAccessRecord cephAccessRecord = new CephAccessRecord();
                    cephAccessRecord.access_key = line;
                    return cephAccessRecord;
                });


        //8.读取HBase中user表，进行维度关联
        SingleOutputStreamOperator<CephAccessRecord> record = AsyncDataStream.unorderedWait(
                validDS,
                new DimAsyncFunction<CephAccessRecord>() {
                    @Override
                    public String getKey(CephAccessRecord record) {
                        return record.access_key;
                    }
                },
                60, TimeUnit.SECONDS);


        record.map(new MapFunction<CephAccessRecord, UserInfo>() {

            UserInfo userInfo = new UserInfo();

            @Override
            public UserInfo map(CephAccessRecord record) throws Exception {
                userInfo.s3_user_id = record.s3_user_id;
                userInfo.display_name = record.display_name;
                userInfo.suspended = record.suspended;
                userInfo.max_buckets = record.max_buckets;
                userInfo.op_mask =record.op_mask;
                userInfo.default_placement = record.default_placement;
                userInfo.default_storage_class = record.default_storage_class;
                userInfo.placement_tags =record.placement_tags;
                userInfo.access_key = record.access_key;
//                BeanUtils.copyProperties(userInfo, record);
                System.out.println(userInfo.toString());
                return userInfo;
            }
        }).addSink(
                ClickHouseUtil.<CephAccessRecord>getJdbcSink("insert into ceph_meta.dwd_ceph_user_info " +
                        "(s3_user_id,display_name,suspended,max_buckets,op_mask,default_placement,default_storage_class,placement_tags,access_key)" +
                        " values(?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }
}
