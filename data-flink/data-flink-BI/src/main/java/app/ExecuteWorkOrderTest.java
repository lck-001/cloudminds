package app;


import bean.WorkOrderData;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import function.SearchFlatMapProduceTaskFunctionTest;
import function.SearchFlatMapProgressReportFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import utlis.ClickHouseUtil;
import utlis.HttpSourceUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class ExecuteWorkOrderTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);


        ParameterTool propertiesargs = ParameterTool.fromArgs(args);
        String fileName = propertiesargs.get("http_conf_path");


        //从本地文件获取动态参数配置文件
        ParameterTool parameters = null;
        try {
            Properties props = new Properties();
            InputStream inputStream = new FileInputStream(fileName);
            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
            props.load(bf);
            parameters = ParameterTool.fromMap((Map) props);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //提升全局变量
        env.getConfig().setGlobalJobParameters(parameters);
        new RequestConf(parameters);


        //2.设置CK&状态后端
        env.enableCheckpointing(5 * 60 * 1000);// 每 ** ms 开始一次 checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);// Checkpoint 必须在** ms内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2 * 60 * 1000);// 确认 checkpoints 之间的时间会进行 ** ms
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//重启策略：重启3次，间隔10s

        // 设置 checkpoint 的并发度为 1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);


        // 获取token api，并获取工位列表
        SingleOutputStreamOperator<String> dataStream = env.addSource(new HttpSourceUtil(RequestConf.STATION_LIST_URL)).uid("http-source").name("http source");


//        dataStream.map(new RichMapFunction<String, List<WorkOrderData>>() {
//            @Override
//            public List<WorkOrderData> map(String s) throws Exception {
//                List<WorkOrderData> workOrderDataList = null;
//                WorkOrderData workOrderData = new WorkOrderData();
//                JSONArray list = JSONObject.parseObject(s).getJSONObject("data").getJSONObject("data").getJSONArray("list");
//                ArrayList<Long> ids = new ArrayList<>();
//                for (Object o : list) {
//                    workOrderData.id = JSONObject.parseObject(JSONObject.toJSONString(o)).getLong("id");
//                    ids.add(workOrderData.id);
//                }
//                RequestConf.EQUIPMENT_ID_LIST = ids;
//                return workOrderDataList;
//            }
//        })
//                .flatMap(new SearchFlatMapProgressReportFunction()).uid("sink-pick-order-detail").name("sink pick order detail")
//                .addSink(ClickHouseUtil.<WorkOrderData>getJdbcSink("insert into ceph_meta.dwd_work_order " +
//                        "(id,workOrderCode,productName,taskStatusMessage,productionDaily,productionHour,progressReportMaterialName)" +
//                        "values(?,?,?,?,?,?,?)"));

        dataStream.flatMap(new RichFlatMapFunction<String, WorkOrderData>() {
            @Override
            public void flatMap(String s, Collector<WorkOrderData> collector) throws Exception {
                WorkOrderData workOrderData = new WorkOrderData();
                JSONArray list = JSONObject.parseObject(s).getJSONObject("data").getJSONObject("data").getJSONArray("list");
                for (Object o : list) {
                    workOrderData.id = JSONObject.parseObject(JSONObject.toJSONString(o)).getLong("id");
                    collector.collect(workOrderData);
                }
            }
        })
                .flatMap(new SearchFlatMapProduceTaskFunctionTest()).uid("sink-pick-order-detail").name("sink pick order detail")
                .addSink(ClickHouseUtil.<WorkOrderData>getJdbcSink("insert into ceph_meta.dwd_work_order " +
                        "(id,workOrderCode,productName,taskStatusMessage,productionDaily,productionHour,progressReportMaterialName,actualStartTime,planStartTime,planEndTime)" +
                        "values(?,?,?,?,?,?,?,?,?,?)"));


        env.execute("pick-order");
    }
}
