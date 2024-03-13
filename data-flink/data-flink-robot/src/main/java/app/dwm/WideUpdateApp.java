package app.dwm;

import app.function.CustomerDeserialization;
import app.function.DimAsyncFunction;
import bean.DeviceWide;
import bean.Relationship;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class WideUpdateApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.获取数据源  并转换为JavaBean对象&提取时间戳生成WaterMark
        //t_user_rcu_robot
        DebeziumSourceFunction<String> relationshipDS = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.t_user_rcu_robot")
                .deserializer(new CustomerDeserialization())
                .serverTimeZone("UTC")
                .startupOptions(StartupOptions.initial())
                .build();

        SingleOutputStreamOperator<JSONObject> typeDS = env.addSource(relationshipDS)
                .map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出数据的操作类型
                        String type = value.getString("type");
                        return !"update".equals(type);
                    }
                });


        AsyncDataStream.unorderedWait(
                typeDS,
                new DimAsyncFunction<JSONObject>("DIM_USER_INFO") {
                    @Override
                    public String getKey(JSONObject orderWide) {
                        return orderWide.getString("tenant_code");
                    }
                    @Override
                    public void join(JSONObject orderWide, JSONObject dimInfo) throws ParseException {
                        String birthday = dimInfo.getString("BIRTHDAY");
                    }
                },
                60,
                TimeUnit.SECONDS);


        env.execute();
    }
}
