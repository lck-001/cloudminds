package function;

import bean.PickOrderDetail;
import bean.PickOrderList;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import utlis.HttpClientUtil;

import static utlis.HttpClientUtil.updateToken;

public class SearchMapFunction extends RichMapFunction<PickOrderList, PickOrderDetail> {
    private String tokenStr;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("appKey", RequestConf.APP_KEY);
        jsonObject.put("appSecret", RequestConf.APP_SECRET);
        jsonObject.put("factoryNumber", RequestConf.FACTORY_NUMBER);
        this.tokenStr = jsonObject.toJSONString();
    }

    @Override
    public PickOrderDetail map(PickOrderList pickOrderList) throws Exception {
        // 从 redis 获取 token
        PickOrderDetail pickOrderDetail;
        while (true) {
            JSONObject params = new JSONObject();
            params.put("pickOrderId", pickOrderList.pickOrderId);
            String jsonStr = HttpClientUtil.httpGet(RequestConf.PICK_ORDER_DETAIL,params.toJSONString());
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            Integer code = jsonObject.getInteger("code");
            if (code == 200) {
                JSONObject data = (JSONObject) jsonObject.getOrDefault("data", new JSONObject());
                pickOrderDetail = JSON.parseObject(String.valueOf(data), PickOrderDetail.class);
                break;
            }else if (code == 10200383 || 3401 == code) {
                //10200383--token到期；3401--token为null（重新获取token）
                updateToken(params);
            }
        }
        return pickOrderDetail;
    }
}
