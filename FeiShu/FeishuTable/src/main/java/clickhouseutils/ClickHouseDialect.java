//package clickhouseutils;
//
//import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
//import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
//import org.apache.flink.table.types.logical.RowType;
//import java.util.Optional;
//
//public class ClickHouseDialect implements JdbcDialect {
//
//    private static final long serialVersionUID = 1L;
//
//    @Override
//    public String dialectName() {
//        return "ClickHouse";
//    }
//
//    @Override
//    public boolean canHandle(String url) {
//        return url.startsWith("jdbc:clickhouse:");
//    }
//
//    @Override
//    public JdbcRowConverter getRowConverter(RowType rowType) {
//        return new ClickHouseRowConverter(rowType);
//    }
//
//    @Override
//    public String getLimitClause(long l) {
//        return "limit num : " + l;
//    }
//
//    @Override
//    public Optional<String> defaultDriverName() {
//        return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
//    }
//
//    @Override
//    public String quoteIdentifier(String identifier) {
//        return "`" + identifier + "`";
//    }
//
//}