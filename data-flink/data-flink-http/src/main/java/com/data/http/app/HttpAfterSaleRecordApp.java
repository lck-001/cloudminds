package com.data.http.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.data.http.bean.AfterSaleRecord;
import com.data.http.bean.Inspection;
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

import static com.data.http.conf.HttpConf.CLICKHOUSE_TABLENAME;
import static com.data.http.conf.HttpConf.FSSTATEBACKEND;

public class HttpAfterSaleRecordApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool propertiesargs = ParameterTool.fromArgs(args);
        String fileName = propertiesargs.get("http_conf_path");
        //从hdfs获取动态参数配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(fileName), conf);
        InputStream wrappedStream = fs.open(new Path(fileName)).getWrappedStream();
        //防止中文乱码
        Properties props = new Properties();
        BufferedReader bf = new BufferedReader(new InputStreamReader(wrappedStream,"UTF-8"));
        props.load(bf);
        ParameterTool parameters = ParameterTool.fromMap((Map) props);

        //从本地文件获取动态参数配置文件
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

        SingleOutputStreamOperator<AfterSaleRecord> flatMap = env.addSource(new HttpUtils()).map(JSON::parseObject)
                .flatMap(new FlatMapFunction<JSONObject, AfterSaleRecord>() {
                    @Override
                    public void flatMap(JSONObject jsonObject, Collector<AfterSaleRecord> collector) throws Exception {
                        AfterSaleRecord afterSaleRecord = new AfterSaleRecord();
                        JSONArray items = jsonObject.getJSONObject("data").getJSONArray("items");
                        for (int i = 0; i < items.toArray().length; i++) {
                            afterSaleRecord.bug_id = items.getJSONObject(i).getJSONObject("fields").getString("BUG 号");
                            afterSaleRecord.bug_name = items.getJSONObject(i).getJSONObject("fields").getString("BUG号");
                            afterSaleRecord.robot_type = items.getJSONObject(i).getJSONObject("fields").getString("产品型号");
                            afterSaleRecord.tenant_name = items.getJSONObject(i).getJSONObject("fields").getString("公司名称");
                            afterSaleRecord.operator = items.getJSONObject(i).getJSONObject("fields").getString("受理人");
                            afterSaleRecord.return_visit = items.getJSONObject(i).getJSONObject("fields").getString("回访详情");
                            JSONArray failureArray = items.getJSONObject(i).getJSONObject("fields").getJSONArray("处理数据-故障描述");
                            for (int i1 = 0; i1 < failureArray.size(); i1++) {
                                afterSaleRecord.failure = failureArray.getJSONObject(i1).getString("text");
                            }
                            JSONArray failureTypeArray = items.getJSONObject(i).getJSONObject("fields").getJSONArray("处理数据-故障类型");
                            for (int i1 = 0; i1 < failureTypeArray.size(); i1++) {
                                afterSaleRecord.failure_type = failureTypeArray.getJSONObject(i1).getString("text");
                            }

                            afterSaleRecord.handle_department = items.getJSONObject(i).getJSONObject("fields").getString("处理部门");
                            afterSaleRecord.remark = items.getJSONObject(i).getJSONObject("fields").getString("备注");
                            afterSaleRecord.status = items.getJSONObject(i).getJSONObject("fields").getString("完成状态");
                            afterSaleRecord.returnvisit_time = items.getJSONObject(i).getJSONObject("fields").getString("客户回访时间");
                            afterSaleRecord.customer_name = items.getJSONObject(i).getJSONObject("fields").getString("客户姓名");
                            afterSaleRecord.customer_source = items.getJSONObject(i).getJSONObject("fields").getString("客户来源");
                            afterSaleRecord.create_time = items.getJSONObject(i).getJSONObject("fields").getString("工单创建时间");
                            afterSaleRecord.finish_time = items.getJSONObject(i).getJSONObject("fields").getString("工单完成时间");
                            afterSaleRecord.known_bug = items.getJSONObject(i).getJSONObject("fields").getString("已知BUG");
                            afterSaleRecord.weeks = items.getJSONObject(i).getJSONObject("fields").getString("当前周数-记录周数");
                            afterSaleRecord.affected_count = items.getJSONObject(i).getJSONObject("fields").getString("影响台数");
                            afterSaleRecord.gender = items.getJSONObject(i).getJSONObject("fields").getString("性别");
                            afterSaleRecord.error_source = items.getJSONObject(i).getJSONObject("fields").getString("报障来源");
                            afterSaleRecord.error_code = items.getJSONObject(i).getJSONObject("fields").getString("故障代码|描述");
                            afterSaleRecord.error_duratin = items.getJSONObject(i).getJSONObject("fields").getString("故障处理时长");
                            afterSaleRecord.error_desc = items.getJSONObject(i).getJSONObject("fields").getString("故障简述");
                            afterSaleRecord.error_type = items.getJSONObject(i).getJSONObject("fields").getString("故障类别");
                            afterSaleRecord.replace_parts = items.getJSONObject(i).getJSONObject("fields").getString("更换备件");
                            afterSaleRecord.service_type = items.getJSONObject(i).getJSONObject("fields").getString("服务类型");
                            afterSaleRecord.uncomplete_reason = items.getJSONObject(i).getJSONObject("fields").getString("未完成原因");
                            afterSaleRecord.robot_id = items.getJSONObject(i).getJSONObject("fields").getString("机身S|N编码");
                            afterSaleRecord.visit_time = items.getJSONObject(i).getJSONObject("fields").getString("来访时间");
                            afterSaleRecord.delivery_type = items.getJSONObject(i).getJSONObject("fields").getString("派单类型");
                            afterSaleRecord.phone = items.getJSONObject(i).getJSONObject("fields").getString("电话");
                            afterSaleRecord.hardcore_version = items.getJSONObject(i).getJSONObject("fields").getString("硬件版本");
                            afterSaleRecord.duration = items.getJSONObject(i).getJSONObject("fields").getString("累计时间");
                            afterSaleRecord.maintain_type = items.getJSONObject(i).getJSONObject("fields").getString("维修类别");
                            afterSaleRecord.maintain_type_new = items.getJSONObject(i).getJSONObject("fields").getString("维修类别 新");
                            afterSaleRecord.maintain_desc = items.getJSONObject(i).getJSONObject("fields").getString("维修详情");
                            afterSaleRecord.maintain_over_reason = items.getJSONObject(i).getJSONObject("fields").getString("维修超7天原因");
                            afterSaleRecord.record_weeks = items.getJSONObject(i).getJSONObject("fields").getString("记录周数");
                            afterSaleRecord.record_year = items.getJSONObject(i).getJSONObject("fields").getString("记录年份");

                            JSONArray recordDateArray = items.getJSONObject(i).getJSONObject("fields").getJSONArray("记录日期");
                            for (int i1 = 0; i1 < recordDateArray.size(); i1++) {
                                afterSaleRecord.record_date = recordDateArray.getJSONObject(i1).getString("text");
                            }
                            afterSaleRecord.address = items.getJSONObject(i).getJSONObject("fields").getString("详细地址");
                            afterSaleRecord.details = items.getJSONObject(i).getJSONObject("fields").getString("详细描述|调查信息");
                            afterSaleRecord.software_version = items.getJSONObject(i).getJSONObject("fields").getString("软件版本");
                            afterSaleRecord.issue_status = items.getJSONObject(i).getJSONObject("fields").getString("问题状态");

                            System.out.println(afterSaleRecord);
                            collector.collect(afterSaleRecord);
                        }
                    }
                });

        flatMap.addSink(
                ClickHouseUtil.<Inspection>getJdbcSink("insert into "+CLICKHOUSE_TABLENAME +
                        "(bug_id,bug_name,robot_type,tenant_name,operator,return_visit,failure,failure_type,handle_department,remark,status,returnvisit_time,customer_name,customer_source,create_time,finish_time,known_bug,weeks,affected_count,gender,error_source,error_code,error_duratin,error_desc,error_type,replace_parts,service_type,uncomplete_reason,robot_id,visit_time,delivery_type,phone,hardcore_version,duration,maintain_type,maintain_type_new,maintain_desc,maintain_over_reason,record_weeks,record_year,record_date,address,details,software_version,issue_status)" +
                        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
