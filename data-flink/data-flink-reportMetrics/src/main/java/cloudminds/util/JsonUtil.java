package cloudminds.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import java.util.Iterator;
import java.util.Map;

public class JsonUtil {
    public static Map<String, Object> analysisJson(String jsonStr, String flag, Map<String, Object> jsonMap) {
        try {
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            Iterator it = jsonObject.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next().toString();
                Object object = jsonObject.get(key);
                if (null != object && isJson(object.toString())) {//如果key中是一个json对象
                    String path = "";
                    if (StringUtils.isNotBlank(flag)) {
//                    path = flag + "." + key;
                        path = flag + "_" + key.replaceAll("\\$", "_D_");
                    } else {
                        path = key.replaceAll("\\$", "_D_");
                    }
//                    jsonMap.put(path, object);
                    analysisJson(object.toString(), path, jsonMap);
                } else {//如果key中是其他
                    String path = "";
                    if (StringUtils.isNotBlank(flag)) {
//                    path = flag + "." + key;
                        path = flag + "_" + key.replaceAll("\\$", "_D_");
                    } else {
                        path = key.replaceAll("\\$", "_D_");
                    }
//                    System.out.println(path + ":" + object.toString() + " ");
                    jsonMap.put(path, object);
                }
            }

        } catch (Exception ignored) {

        }
        return jsonMap;
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
}
