package com.cloudminds.cdc.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JsonHandler {
    public static void analysisJson(String jsonStr, String flag, Map<String, Object> jsonMap) {
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        Iterator it = jsonObject.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next().toString();
            Object object = jsonObject.get(key);
            if (null != object && isJson(object.toString())) {//如果key中是一个json对象
                String path = "";
                if (StringUtils.isNotBlank(flag)) {
//                    path = flag + "." + key;
                    path = flag + "_P_" + key.replaceAll("\\$","_D_");
                } else {
                    path = key.replaceAll("\\$","_D_");
                }
                jsonMap.put(path, object);
                analysisJson(object.toString(), path, jsonMap);
            } else {//如果key中是其他
                String path = "";
                if (StringUtils.isNotBlank(flag)) {
//                    path = flag + "." + key;
                    path = flag + "_P_" + key.replaceAll("\\$","_D_");
                } else {
                    path = key.replaceAll("\\$","_D_");
                }
//                System.out.println(path+":"+object.toString()+" ");
                jsonMap.put(path, object);
            }
        }
    }

    public static boolean isJson(String str) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        Object parse = null;
        try {
            parse = JSON.parse(str);
        } catch (Exception e) {
            return false;
        }
        if (parse instanceof JSONObject) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        String str = "{\"_id\": \"864972049998755\", \"reportAttribute\": {\"rcuService\": {\"versionName\": \"RCUXSERVICE_A_V1.1.1.209_RC_478_SPV1.9.1_202208121732\", \"versionCode\": 478}, \"robot\": {\"RobotType\": \"Ginger\", \"SerialNumber\": \"GNL09S2134001073\", \"RobotId\": \"GINLITXR-1LXXXXXXXXXGNL09S2134001073\", \"softversion\": \"ginger_lite_V4.0.435.1\", \"model\": \"XR-1L\", \"id\": \"GINLITXR-1LXXXXXXXXXGNL09S2134001073\", \"ProductionDate\": \"20210823\", \"HardwareVersion\": \"PVT3.0\", \"version\": \"1.0\"}, \"SCA\": {\"version\": \"\"}, \"mcsClient\": {\"simIccid\": \"8965051503290418257\", \"versionCode\": 0}, \"rcu\": {\"os\": \"H1A1000.080ho.1604\", \"model\": \"H1A1000\", \"manufacturer\": \"CloudMinds\"}, \"micArray\": {\"versionName\": \"VSP_PK_V1.1.38_product_3\"}, \"ue4Client\": {\"versionName\": \"V4.0.0_Lite_09091047_4030\", \"versionCode\": 4030}, \"ECU\": {\"version\": \"{\\\"EcuFmVersion\\\":\\\"ti_ecu_V0.0.375\\\",\\\"EcuHwVersion\\\":\\\"PVT3.0\\\",\\\"EcuId\\\":\\\"192.168.1.4\\\"}\\n    \"}, \"rcuApp\": {\"versionName\": \"CLOUDGINGERLITE_RCUX_A_V1.8.5.7_RC_209_202208301621\", \"versionCode\": 209}, \"timestamp\": {\"$numberLong\": \"1667999052035\"}}, \"_class\": \"com.cloudminds.roc.model.operation.mongo.MongoAttribute\"}";
        HashMap<String, Object> map = new HashMap<>();
        analysisJson(str,"",map);
        System.out.println(map);
    }
}
