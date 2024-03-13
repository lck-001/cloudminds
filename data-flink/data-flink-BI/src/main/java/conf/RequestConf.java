package conf;

import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestConf {
    public static String TOKEN_URL ;
    public static String APP_KEY ;
    public static String APP_SECRET ;
    public static String FACTORY_NUMBER ;
    public static Integer PAGE_NUM = 1;
    public static Integer PAGE_SIZE = 20;
    public static String UPDATE_FROM ;
    public static String UPDATE_TO ;
    public static String PICK_ORDER_DETAIL ;
    public static String PICK_ORDER_LIST ;
    public static String STATION_LIST_URL ;
    public static String PRODUCE_TASK_URL ;
    public static String PROGRESS_ROPORT_URL ;

    public static Integer REQUEST_INTERVAL = 3600000;
    public static String AUTH = "";


    public static List<Long>  EQUIPMENT_ID_LIST ;
    public static Integer[]  TASKSTATUS_LIST = {2};
    public static String[]  SOPTASK_ID_LIST = {};


    public static String CLICKHOUSE_URL;
    public static String CLICKHOUSE_USERNAME;
    public static String CLICKHOUSE_PASSWORD;
    public static String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static String FSSTATEBACKEND;

    public RequestConf(ParameterTool propertiesargs) {
        TOKEN_URL = propertiesargs.get("token_url");
        APP_KEY = propertiesargs.get("app_key");
        APP_SECRET = propertiesargs.get("app_secret");
        FACTORY_NUMBER = propertiesargs.get("factoryNumber");
        PAGE_NUM = Integer.valueOf(propertiesargs.get("page"));
        PAGE_SIZE = Integer.valueOf(propertiesargs.get("size"));
        UPDATE_FROM = propertiesargs.get("update_from");
        UPDATE_TO = propertiesargs.get("update_to");
        PICK_ORDER_DETAIL = propertiesargs.get("pick_order_detail");
        PICK_ORDER_LIST = propertiesargs.get("pick_order_list");
        STATION_LIST_URL = propertiesargs.get("station_list_url");
        PRODUCE_TASK_URL = propertiesargs.get("produce_task_url");
        PROGRESS_ROPORT_URL = propertiesargs.get("progress_report_url");
        REQUEST_INTERVAL = Integer.valueOf(propertiesargs.get("request_interval"));
        CLICKHOUSE_URL = propertiesargs.get("clickhouse_url");
        CLICKHOUSE_USERNAME = propertiesargs.get("clickhouse_username");
        CLICKHOUSE_PASSWORD = propertiesargs.get("clickhouse_password");
        FSSTATEBACKEND = propertiesargs.get("fs_stateBackend");
    }
}