package com.cloudminds.kafka.jdbc.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author luojun
 */
public class OplogHandler {
    public static JSONObject getJsonMap(String jsonStr) {
        JSONObject json = new JSONObject();
        JSONReader reader = new JSONReader(new StringReader(jsonStr));
        reader.startObject();
        while (reader.hasNext()) {
            String key = reader.readString();
            if ("o".equals(key)) {
                reader.startObject();
                while (reader.hasNext()) {
                    String key1 = reader.readString();
                    if ("$set".equals(key1)) {
                        reader.startObject();
                        while (reader.hasNext()) {
                            String key3 = reader.readString();
                            Object obj = reader.readObject();
                            if (obj == null) {
                                json.put(key3, "");
                            } else {
                                json.put(key3, obj);
                            }
                        }
                        reader.endObject();
                    } else if ("$v".equals(key1)) {
                        String v = reader.readObject().toString();
                    } else {
                        Object obj = reader.readObject();
                        if (obj == null) {
                            json.put(key1, "");
                        } else {
                            json.put(key1, obj);
                        }
                    }
                }
                reader.endObject();
            } else if ("o2".equals(key)) {
                Object obj = reader.readObject();
                if (obj != null) {
                    HashMap hashMap = JSON.parseObject(obj.toString(), HashMap.class);
                    json.putAll(hashMap);
                }
            } else if ("_id".equals(key)){
                Object obj = reader.readObject();
                if (obj != null) {
                    HashMap hashMap = JSON.parseObject(obj.toString(), HashMap.class);
                    json.putAll(hashMap);
                }
            } else {
                Object obj = reader.readObject();
                if (obj == null) {
                    json.put(key, "");
                } else {
                    json.put(key, obj);
                }
            }
        }
        reader.endObject();

        return json;
    }
    public static void  analysisJson(String jsonStr,String flag, Map<String,Object> jsonMap) {
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        Iterator it = jsonObject.keySet().iterator();
        while(it.hasNext()) {
            String key = it.next().toString();
            Object object = jsonObject.get(key);
            if(null != object && isJson(object.toString())) {//如果key中是一个json对象
                String path = "";
                if (StringUtils.isNotBlank(flag)) {
                    path = flag + "." + key;
                } else {
                    path = key;
                }
                jsonMap.put(path,object);
                analysisJson(object.toString(),path,jsonMap);
            } else {//如果key中是其他
                String path = "";
                if (StringUtils.isNotBlank(flag)) {
                    path = flag + "." + key;
                } else {
                    path = key;
                }
//                System.out.println(path+":"+object.toString()+" ");
                jsonMap.put(path,object);
            }
        }
    }
    public static boolean isJson(String str){
        if (StringUtils.isBlank(str)){
            return false;
        }
        Object parse = null;
        try {
            parse = JSON.parse(str);
        } catch (Exception e) {
            return false;
        }
        if (parse instanceof JSONObject ){
            return true;
        }
        return false;
    }

}
