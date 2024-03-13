package function;

import bean.WorkOrderData;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static utlis.HttpClientUtil.httpGet;
import static utlis.HttpClientUtil.updateToken;

public class SearchMapProduceTaskFunction extends RichMapFunction<WorkOrderData, WorkOrderData> {
    private JSONObject tokenStr;
    private JSONObject params;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("appKey", RequestConf.APP_KEY);
        jsonObject.put("appSecret", RequestConf.APP_SECRET);
        jsonObject.put("factoryNumber", RequestConf.FACTORY_NUMBER);
        this.tokenStr = jsonObject;

        JSONObject params = new JSONObject();

        params.put("taskStatusList", RequestConf.TASKSTATUS_LIST);
        params.put("sopTaskIdList", RequestConf.SOPTASK_ID_LIST);
        params.put("size", 20);


        ArrayList<HashMap<String, String>> sorter = new ArrayList<HashMap<String, String>>();
        HashMap<String, String> hm1 = new HashMap<String, String>();
        hm1.put("field", "taskCode");
        hm1.put("order", "desc");
        sorter.add(hm1);
        params.put("sorter", sorter);

        this.params = params;


    }

    @Override
    public WorkOrderData map(WorkOrderData workOrderData) throws Exception {
        // 从 redis 获取 token
//        params.put("equipmentIdList", RequestConf.EQUIPMENT_ID_LIST);//1670628112690218   1670628112690223
//        RequestConf.EQUIPMENT_ID_LIST.add(workOrderData.id);
        List<Long> ids = new ArrayList<>();
        ids.add(workOrderData.id);
        params.put("equipmentIdList", ids);
        WorkOrderData newWorkOrderData = null;
        while (true) {

            String jsonStr = httpGet(RequestConf.PRODUCE_TASK_URL, params.toJSONString());
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            Integer code = jsonObject.getInteger("code");
            if (code == 200) {
                JSONArray jsonArray = jsonObject.getJSONObject("data").getJSONArray("list");
                int list_size = jsonArray.size();
                if (list_size == 0) {
                    //判断当前数据处理完成，修改时间参数，休眠
                    break;
                } else {
                    for (int i = 0; i < jsonArray.size(); i++) {
                        newWorkOrderData.id = workOrderData.id;
                        newWorkOrderData.workOrderCode = jsonArray.getJSONObject(i).getString("workOrderCode");
                        newWorkOrderData.taskStatusMessage = jsonArray.getJSONObject(i).getJSONObject("taskStatus").getString("message");
                        newWorkOrderData.productName = jsonArray.getJSONObject(i).getJSONObject("taskMaterial").getJSONObject("baseInfo").getString("name");
                        System.out.println("PRODUCE_TASK_URL============" + newWorkOrderData.workOrderCode + "  " + newWorkOrderData.taskStatusMessage + "  " + newWorkOrderData.productName);
                    }
                }
            } else if (code == 10200383 || 3401 == code) {
                //10200383--token到期；3401--token为null（重新获取token）
                updateToken(tokenStr);
            }
        }
        return newWorkOrderData;
    }
}