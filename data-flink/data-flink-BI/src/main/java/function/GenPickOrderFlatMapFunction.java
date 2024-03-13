package function;

import bean.PickOrderList;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class GenPickOrderFlatMapFunction implements FlatMapFunction<String, PickOrderList> {
    @Override
    public void flatMap(String s, Collector<PickOrderList> collector) throws Exception {
        PickOrderList pickOrderList = new PickOrderList();
        JSONArray list = JSONObject.parseObject(s).getJSONObject("data").getJSONArray("list");
        for (Object o : list) {
            pickOrderList = JSONObject.parseObject(JSONObject.toJSONString(o), PickOrderList.class);
            //处理Long、Integer == null 的情况
            pickOrderList.requirementTime = pickOrderList.requirementTime == null ? 946656000000L : pickOrderList.requirementTime;
            pickOrderList.line = pickOrderList.line == null ? 0 : pickOrderList.line;
            pickOrderList.createdAt = pickOrderList.createdAt == null ? 0 : pickOrderList.createdAt;
            pickOrderList.seq = pickOrderList.seq == null ? 0 : pickOrderList.seq;
            pickOrderList.updatedAt = pickOrderList.updatedAt == null ? 0 : pickOrderList.updatedAt;
            pickOrderList.pickOrderId = pickOrderList.pickOrderId == null ? 0 : pickOrderList.pickOrderId;
            pickOrderList.targetWarehouseId = pickOrderList.targetWarehouseId == null ? 0 : pickOrderList.targetWarehouseId;
            pickOrderList.alternativeMaterialFlag = pickOrderList.alternativeMaterialFlag == null ? 0 : pickOrderList.alternativeMaterialFlag;
            pickOrderList.inputOrAlternativeMaterialId = pickOrderList.inputOrAlternativeMaterialId == null ? 0 : pickOrderList.inputOrAlternativeMaterialId;
            pickOrderList.inputProcessId = pickOrderList.inputProcessId == null ? 0 : pickOrderList.inputProcessId;
            pickOrderList.workOrderId = pickOrderList.workOrderId == null ? 0 : pickOrderList.workOrderId;
            pickOrderList.pickOrderDetailId = pickOrderList.pickOrderDetailId == null ? 0 : pickOrderList.pickOrderDetailId;
            pickOrderList.sourceWarehouseId = pickOrderList.sourceWarehouseId == null ? 0 : pickOrderList.sourceWarehouseId;
            collector.collect(pickOrderList);
        }
    }
}
