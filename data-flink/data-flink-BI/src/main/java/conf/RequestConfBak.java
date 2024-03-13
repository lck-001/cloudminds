package conf;

import org.apache.flink.api.java.utils.ParameterTool;

public class RequestConfBak {
    public static String TOKEN_URL = "https://v3-ali.blacklake.cn/api/openapi/domain/web/v1/system/_refreshToken";
//    public static String APP_KEY = "dtjkzh";
//    public static String APP_SECRET = "$argon2id$v=19$m=65536,t=1,p=4$S8KCqfJl9eWPo/lW1p2IaXnmlmk3Sm08zCAcJWPyZbzEv3HH/CBo75z78ghWinRJIG7l0dEBXqEiIa5Dd40qv0XjvyGF++EsokfEHW80LsaKmUGrGqbfkKosvvL9mc9o/DwoTNPLjNtbXmt+7KJpLCuTIGZGLJXk98XXUIQoZmI$6dxzO5403CwT9fUiU9XcTdYP6gRn2poksFfIh2d6CSFCyJnPeUTxdXeckAzHkh5TmP7uz/e/ie5XSwF/Nwsiqa3p9AnK0iutEkIEizUWUCAXOi5l4wvwfjKp0ddD3OR8M8YnYqer2gjiX2KUQIlmDzYWZEJa0f3Moa3N9miTbnrUJsXBWgFAsZmn5tgmVP53225X3DpqEA3fi2Odq/SPLQMt+Uk+3mX1cOH/XwsFu156TVL+I1WK3+aBgAruCuOAxeSTCLeoz7WrzZhKB2TfWYWNfPzcPwsfHqv1GLpmxaX//wWPCGVMZFXzEITCe+C0cJrjQs9vxRdgR3nro1iMrg";
//    public static String FACTORY_NUMBER = "03065911";
    public static String APP_KEY = "bidata_jkzh";
    public static String APP_SECRET = "$argon2id$v=19$m=65536,t=1,p=4$hcJIjgMaleMnplCJ0HXQZIWri2QY2nFSSutbDwGZ8CboktEV0ItwG2815fvp82KS9JAT5SL8DXKkTosQd8Pp9ZmT5vmtlYmGZzdB2NpVG26jr3YM/NR6glRmXIAp4D/lrGtK0qLpt35nRGy1iO+LckystV5lBioTzK47JUotI/A$N4YmQ1RRvPfoK2oh7F0/asP+pOsG4uA2uJ0E/BngJvh3U8f5ZZ5Wdh1DhVgZHNSiBPLWN4vN6DpeA0WiORE05zjtUC8RCPvn4Pi4JdUKvcMtts+Ci+pBs9n7INO3LDZSaP1KqSGnT17DCI909wD8yCZxurZO9YAbIf09dMuRuO/43MeErXXo4pc6E05fR6gIqO1nD91qpF5b3tiaCxDtCL53BKgYkcKETR8LpfV9qYkWoe+Q0x0n0s1Calj7k2sVXZpZ4t9GwmwGQHKcROYU4HBP/W4WHD0H5nPL4vjLuFplO4bG4EewzHBIg9yMdhoxZK9fiuPSZWsklxvOxYXcTQ";
    public static String FACTORY_NUMBER = "03065911";
    public static Integer PAGE_NUM = 1;
    public static Integer PAGE_SIZE = 20;
//    public static String UPDATE_FROM = "1681806547000";
//    public static String UPDATE_TO = "1681813747000";
    public static String UPDATE_FROM = "1695052800000";
    public static String UPDATE_TO = "1695093558932";
//    public static String PICK_ORDER_DETAIL = "https://v3-ali.blacklake.cn/api/openapi/domain/web/v1/route/med/open/v1/pick_order/_detail";
//    public static String PICK_ORDER_DETAIL = "https://v3-ali.blacklake.cn/api/resource/domain/web/v1/resources/screen/list";
    public static String PICK_ORDER_DETAIL = "https://v3-ali.blacklake.cn/api/mfg/domain/web/v1/produce_task/_list";
//    public static String PICK_ORDER_LIST = "https://v3-ali.blacklake.cn/api/openapi/domain/web/v1/route/med/open/v1/pick_order/_list";
//    public static String PICK_ORDER_LIST = "https://v3-ali.blacklake.cn/api/resource/domain/web/v1/resources/screen/list";
    public static String PICK_ORDER_LIST = "https://v3-ali.blacklake.cn/api/mfg/domain/web/v1/produce_task/_list";
    public static Integer REQUEST_INTERVAL = 3600000;
    public static String AUTH = "";


    public static Long[]  EQUIPMENT_ID_LIST = {1670628112690213L};
    public static Integer[]  TASKSTATUS_LIST = {2};


    public static String CLICKHOUSE_URL;
    public static String CLICKHOUSE_USERNAME;
    public static String CLICKHOUSE_PASSWORD;
    public static String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static String FSSTATEBACKEND;

    public RequestConfBak(ParameterTool propertiesargs) {
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
        REQUEST_INTERVAL = Integer.valueOf(propertiesargs.get("request_interval"));
        CLICKHOUSE_URL = propertiesargs.get("clickhouse_url");
        CLICKHOUSE_USERNAME = propertiesargs.get("clickhouse_username");
        CLICKHOUSE_PASSWORD = propertiesargs.get("clickhouse_password");
        FSSTATEBACKEND = propertiesargs.get("fs_stateBackend");
    }
}