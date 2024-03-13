package function;

import bean.WorkOrderData;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import utlis.DateUtils;
import utlis.HttpClientUtil;

import java.util.ArrayList;
import java.util.HashMap;

import static utlis.HttpClientUtil.updateToken;

public class SearchMapProgressReportFunction extends RichMapFunction<WorkOrderData, WorkOrderData> {
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

        // new sorter 参数
        ArrayList<HashMap<String, String>> sorter = new ArrayList<HashMap<String, String>>();
        HashMap<String, String> hm1 = new HashMap<String, String>();
        hm1.put("field","reportTime");
        hm1.put("order","desc");
        sorter.add(hm1);

        JSONObject params = new JSONObject();
        params.put("page", 1);
        params.put("size", 20);
        params.put("sorter", sorter);
        params.put("reportTimeFrom", DateUtils.getStartTime());
        params.put("reportTimeTo", DateUtils.getEndTime());

        this.params = params;
    }

    @Override
    public WorkOrderData map(WorkOrderData workOrderData) throws Exception {
        // 从 redis 获取 token
        WorkOrderData newWorkOrderData;
        while (true) {
            params.put("equipmentIdList", RequestConf.EQUIPMENT_ID_LIST);//1670628112690218   1670628112690223
//            params.put("equipmentIdList", new Long[]{wokeOrderData.id});

            String jsonStr = HttpClientUtil.httpGet(RequestConf.PROGRESS_ROPORT_URL,params.toJSONString());
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            Integer code = jsonObject.getInteger("code");
            if (code == 200) {
                JSONObject data = (JSONObject) jsonObject.getOrDefault("data", new JSONObject());
                newWorkOrderData = JSON.parseObject(String.valueOf(data), WorkOrderData.class);
                if (data.getJSONArray("list").size()>0){
                    JSONArray list = data.getJSONArray("list");
                    for (int i = 0; i < list.size(); i++) {
                        newWorkOrderData.productionDaily = data.getJSONArray("list").getJSONObject(i).getJSONObject("progressReportAmount1").getDouble("amount");
                        newWorkOrderData.progressReportMaterialName = data.getJSONArray("list").getJSONObject(i).getString("progressReportMaterialName");
                    }
                }
                System.out.println("PROGRESS_ROPORT_URL====" + newWorkOrderData.productionDaily +"  "+ newWorkOrderData.progressReportMaterialName);
                break;
            }else if (code == 10200383 || 3401 == code) {
                //10200383--token到期；3401--token为null（重新获取token）
                updateToken(tokenStr);
            }
        }
        return newWorkOrderData;
    }
}
