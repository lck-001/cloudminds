package app;


import bean.WorkOrderData;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import function.GenExecuteWorkOrderFlatMapFunction;
import function.SearchMapProduceTaskFunction;
import function.SearchMapProgressReportFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utlis.HttpSourceUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

//import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;

public class ExecuteWorkOrder {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);


        ParameterTool propertiesargs = ParameterTool.fromArgs(args);
        String fileName = propertiesargs.get("http_conf_path");
//        //从hdfs获取动态参数配置文件
//        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(fileName), conf);
//        InputStream wrappedStream = fs.open(new Path(fileName)).getWrappedStream();
//        //防止中文乱码
//        Properties props = new Properties();
//        BufferedReader bf = new BufferedReader(new InputStreamReader(wrappedStream,"UTF-8"));
//        props.load(bf);
//        ParameterTool parameters = ParameterTool.fromMap((Map) props);


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
//        env.setStateBackend(new FsStateBackend(FSSTATEBACKEND));
        env.enableCheckpointing(5*60*1000);// 每 ** ms 开始一次 checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 设置模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(10*60*1000);// Checkpoint 必须在** ms内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2*60*1000);// 确认 checkpoints 之间的时间会进行 ** ms
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//重启策略：重启3次，间隔10s

        // 设置 checkpoint 的并发度为 1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);


        // 获取token api，并获取工位列表
        SingleOutputStreamOperator<String> dataStream = env.addSource(new HttpSourceUtil(RequestConf.STATION_LIST_URL)).uid("http-source").name("http source");

        // 获取工位设备id
        SingleOutputStreamOperator<WorkOrderData> wokeOrderId = dataStream.flatMap(new GenExecuteWorkOrderFlatMapFunction()).uid("flat-source").name("flat source");


        wokeOrderId.print("dataStream++++++++++++++++++++");

//        pickOrder.addSink(ClickHouseUtil.<PickOrderList>getJdbcSink("insert into cross.dwd_df_pick_order_list " +
//                "(warehouseIssuedAmount,requirementTime,receivePickAmount,line,workOrderCode,pickOrderStatus,inputProcessName,operator,createdAt,pickOrderIssuedAmount,targetWarehouse,pickOrderCode,supplierList,seq,updatedAt,creator,pickOrderType,inputProcessCode,workCenter,pickOrderId,requestPickAmount,material,targetWarehouseId,alternativeMaterialFlag,inputOrAlternativeMaterialId,inputProcessId,workOrderId,pickOrderDetailId,mainOutputBatchNumber,transferIssuedAmount,targetWarehouseName,sourceWarehouseId,sourceWarehouseName,sourceWarehouse,batchCode,productionDepartment,remark,transferOrderVO)" +
//                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //获取正在执行的任务列表
        SingleOutputStreamOperator<WorkOrderData> produceTaskStream = wokeOrderId.map(new SearchMapProduceTaskFunction()).uid("sink-pick-order-detail").name("sink pick order detail");

        // 获取当天报工记录
//        SingleOutputStreamOperator<WorkOrderData> progressReportHour = wokeOrderId.map(new SearchMapProgressReportFunction()).uid("sink-progress-report-daily").name("sink pick order detail");

//        pickOrderDetail.addSink(ClickHouseUtil.<PickOrderDetail>getJdbcSink("insert into cross.dwd_df_pick_order_detail " +
//                "(pickOrderCode,pickOrderId,pickOrderType,pickOrderStatus,materials,remark,creator,createdAt,operator,updatedAt)" +
//                "values(?,?,?,?,?,?,?,?,?,?)"));


        env.execute("pick-order");
    }
}
