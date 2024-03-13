package com.data.http.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.data.http.bean.Virtual;
import com.data.http.conf.HttpConf;
import com.data.http.utlis.ClickHouseUtil;
import com.data.http.utlis.HttpUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.data.http.conf.HttpConf.*;

public class HttpVirtualApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool propertiesargs = ParameterTool.fromArgs(args);
        String fileName = propertiesargs.get("http_conf_path");

//        //从本地文件获取动态参数配置文件
//        ParameterTool parameters = null;
//        try {
//            Properties props = new Properties();
//            InputStream inputStream = new FileInputStream(fileName);
//            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
//            props.load(bf);
//            parameters = ParameterTool.fromMap((Map) props);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        //提升全局变量
//        env.getConfig().setGlobalJobParameters(parameters);
//        new HttpConf(parameters);

        //从hdfs获取动态参数配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(fileName), conf);
        InputStream wrappedStream = fs.open(new Path(fileName)).getWrappedStream();
        //防止中文乱码
        Properties props = new Properties();
        BufferedReader bf = new BufferedReader(new InputStreamReader(wrappedStream,"UTF-8"));
        props.load(bf);
        ParameterTool parameters = ParameterTool.fromMap((Map) props);
        //提升全局变量
        env.getConfig().setGlobalJobParameters(parameters);
        new HttpConf(parameters);


        //2.设置CK&状态后端
        env.setStateBackend(new FsStateBackend(FSSTATEBACKEND));
        env.enableCheckpointing(10000);// 每 ** ms 开始一次 checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(100000);// Checkpoint 必须在** ms内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);// 确认 checkpoints 之间的时间会进行 ** ms
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//重启策略：重启3次，间隔10s

        SingleOutputStreamOperator<Virtual> flatMap = env.addSource(new HttpUtils()).map(JSON::parseObject)
                .flatMap(new FlatMapFunction<JSONObject, Virtual>() {
                    @Override
                    public void flatMap(JSONObject jsonObject, Collector<Virtual> collector) throws Exception {
                        Virtual virtual = new Virtual();
                        JSONArray items = jsonObject.getJSONObject("data").getJSONArray("items");
                        for (int i = 0; i < items.toArray().length; i++) {
                            String robot = items.getJSONObject(i).getJSONObject("fields").getString("Robot ID");
                            String tenant = items.getJSONObject(i).getJSONObject("fields").getString("客户编码");
                            String user = items.getJSONObject(i).getJSONObject("fields").getString("账号名称");
                            virtual.robot_id = (robot == null) ? "" : robot.trim().replaceAll(" ", "");
                            virtual.user_email = items.getJSONObject(i).getJSONObject("fields").getJSONObject("姓名").getString("email");
                            virtual.create_user = items.getJSONObject(i).getJSONObject("fields").getJSONObject("姓名").getString("name");
                            virtual.screen_status = items.getJSONObject(i).getJSONObject("fields").getString("前端屏幕显示");
                            virtual.harix_status = items.getJSONObject(i).getJSONObject("fields").getString("Hari状态");
                            virtual.tts_status = items.getJSONObject(i).getJSONObject("fields").getString("TTS及现场声音");
                            virtual.recover_status = items.getJSONObject(i).getJSONObject("fields").getString("远程重启操作是否恢复");
                            virtual.tenant_code = (tenant == null) ? "" : tenant.trim().replaceAll(" ", "");
                            virtual.jira = items.getJSONObject(i).getJSONObject("fields").getString("异常情况注明jira编号");
                            virtual.user_name = (user == null) ? "" : user.trim().replaceAll(" ", "");
                            virtual.event_time = (items.getJSONObject(i).getJSONObject("fields").getLong("时间")) / 1000;
                            virtual.remark = items.getJSONObject(i).getJSONObject("fields").getString("备注");

                            collector.collect(virtual);
                        }
                    }
                });

        flatMap.addSink(
                        ClickHouseUtil.<Virtual>getJdbcSink("insert into " + CLICKHOUSE_TABLENAME +
                                "(event_time,create_user,tenant_code,user_name,robot_id,screen_status,harix_status,tts_status,recover_status,jira,remark,user_email,robot_type)" +
                                " values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
