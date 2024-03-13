package function;

import bean.WorkOrderData;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


public class GenExecuteWorkOrderFlatMapFunction implements FlatMapFunction<String, WorkOrderData> {
    @Override
    public void flatMap(String s, Collector<WorkOrderData> collector) throws Exception {
        WorkOrderData workOrderData = new WorkOrderData();
        JSONArray list = JSONObject.parseObject(s).getJSONObject("data").getJSONObject("data").getJSONArray("list");
        ArrayList<Long> ids = new ArrayList<>();
        for (Object o : list) {
            workOrderData = JSONObject.parseObject(JSONObject.toJSONString(o), WorkOrderData.class);
            //处理Long、Integer == null 的情况
            workOrderData.id = workOrderData.id == null ? 946656000000L : workOrderData.id;
            ids.add(workOrderData.id);
//            collector.collect(workOrderData);
        }
        RequestConf.EQUIPMENT_ID_LIST = ids;
        collector.collect(workOrderData);
    }
}
