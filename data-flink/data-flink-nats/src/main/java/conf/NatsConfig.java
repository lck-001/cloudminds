package conf;

import org.apache.flink.api.common.ExecutionConfig;

import java.io.IOException;
import java.util.Map;

public class NatsConfig {

    public static String NATS_URL ;
    public static String NATS_TOPIC ;
    public static String CLICKHOUSE_URL ;
    public static String CLICKHOUSE_USERNAME ;
    public static String CLICKHOUSE_PASSWORD ;
    public static String CLICKHOUSE_TABLENAME ;
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static String IPFILE_PATH ;
    public static String FSSTATEBACKEND ;
    public static String HDFS_FILE_PATH ;

    public NatsConfig(ExecutionConfig.GlobalJobParameters parameters) throws IOException {
        Map<String, String> map = parameters.toMap();
        NATS_URL = map.get("nats_url");
        NATS_TOPIC = map.get("nats_topic");
        CLICKHOUSE_URL = map.get("clickhouse_url");
        CLICKHOUSE_USERNAME = map.get("clickhouse_username");
        CLICKHOUSE_PASSWORD = map.get("clickhouse_password");
        CLICKHOUSE_TABLENAME = map.get("clickhouse_tablename");
        IPFILE_PATH = map.get("ip_file_path");
        FSSTATEBACKEND = map.get("fs_stateBackend");
        HDFS_FILE_PATH = map.get("hdfs_file_path");
    }
}
