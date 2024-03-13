package cloudminds.conf;

import org.apache.flink.api.common.ExecutionConfig;

import java.io.IOException;
import java.util.Map;

public class CephConfig {

    public static String KAFKA_SOURCE_SERVER ;
    public static String KAFKA_SOURCE_OFFSET ;
    public static String CLICKHOUSE_URL ;
    public static String CLICKHOUSE_USERNAME ;
    public static String CLICKHOUSE_PASSWORD ;
    public static String CLICKHOUSE_TABLENAME ;
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static String FSSTATEBACKEND ;
    public static String KAFKA_SOURCE_TOPIC ;
    public static String KAFKA_SOURCE_GROUP ;

    public CephConfig(ExecutionConfig.GlobalJobParameters parameters) throws IOException {
        Map<String, String> map = parameters.toMap();
        KAFKA_SOURCE_SERVER = map.get("kafka_source_quorum");
        KAFKA_SOURCE_TOPIC = map.get("kafka_source_topic");
        KAFKA_SOURCE_GROUP = map.get("kafka_source_group");
        KAFKA_SOURCE_OFFSET = map.get("kafka_source_offset");
        CLICKHOUSE_URL = map.get("clickhouse_url");
        CLICKHOUSE_USERNAME = map.get("clickhouse_username");
        CLICKHOUSE_PASSWORD = map.get("clickhouse_password");
        CLICKHOUSE_TABLENAME = map.get("clickhouse_tablename");
        FSSTATEBACKEND = map.get("fs_stateBackend");
    }
}
