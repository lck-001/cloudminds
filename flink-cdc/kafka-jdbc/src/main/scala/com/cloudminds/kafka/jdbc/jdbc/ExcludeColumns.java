package com.cloudminds.kafka.jdbc.jdbc;

import java.util.HashMap;
import java.util.Map;

public class ExcludeColumns {
    public static Map createMap() {
         Map excludeColumns = new HashMap(){{
            put("db","1");
            put("table","1");
//            put("bigdata_method","1");
//            put("event_time","1");
//            put("k8s_env_name","1");
        }};
        return excludeColumns;
    }
}
