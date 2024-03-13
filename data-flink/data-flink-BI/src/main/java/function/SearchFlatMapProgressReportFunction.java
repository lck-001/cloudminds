package function;

import bean.WorkOrderData;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import utlis.DateUtils;
import utlis.HttpClientUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static utlis.HttpClientUtil.httpGet;
import static utlis.HttpClientUtil.updateToken;

public class SearchFlatMapProgressReportFunction extends RichFlatMapFunction<List<WorkOrderData>, WorkOrderData> {
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
        hm1.put("field", "reportTime");
        hm1.put("order", "desc");
        sorter.add(hm1);

        JSONObject params = new JSONObject();
        params.put("page", 1);
        params.put("size", 20);
        params.put("sorter", sorter);
//        params.put("reportTimeFrom", DateUtils.getStartTime());
//        params.put("reportTimeTo", DateUtils.getEndTime());
        params.put("reportTimeFrom", DateUtils.getHourStartTime());
        params.put("reportTimeTo", DateUtils.getHourEndTime());

        this.params = params;
    }


    @Override
    public void flatMap(List<WorkOrderData> workOrderData, Collector<WorkOrderData> collector) throws Exception {
        WorkOrderData newWorkOrderData = new WorkOrderData();
        while (true) {
            params.put("equipmentIdList", RequestConf.EQUIPMENT_ID_LIST);//1670628112690218   1670628112690223

            String jsonStr = HttpClientUtil.httpGet(RequestConf.PROGRESS_ROPORT_URL, params.toJSONString());
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
                        newWorkOrderData.id = jsonArray.getJSONObject(i).getJSONArray("equipments").getJSONObject(0).getLong("id");
                        newWorkOrderData.productionDaily = jsonArray.getJSONObject(i).getJSONObject("progressReportAmount1").getDouble("amount");
                        newWorkOrderData.progressReportMaterialName = jsonArray.getJSONObject(i).getString("progressReportMaterialName");
                        collector.collect(newWorkOrderData);
                        System.out.println("PROGRESS_ROPORT_URL====" + newWorkOrderData.productionDaily + "  " + newWorkOrderData.progressReportMaterialName);
                    }
                }
                break;
            } else if (code == 10200383 || 3401 == code) {
                //10200383--token到期；3401--token为null（重新获取token）
                updateToken(tokenStr);
            }
        }
    }
}
