import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SocketApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)" +
                "WITH ('connector' = 'socket','hostname' = 'localhost','port' = '9999','byte-delimiter' = '10','format' = 'changelog-csv','changelog-csv.column-delimiter' = '|')");
        Table table= tableEnv.sqlQuery("SELECT name, SUM(score) FROM UserScores GROUP BY name");


//        DataStream<Tuple2<Boolean, user>> stream = tableEnv.toRetractStream(table, user.class);
//        stream.print();
//        DataStream<Tuple2<Boolean, user>> stream = tableEnv.toRetractStream(table, user.class);
//        DataStream<user> stream = tableEnv.toAppendStream(table, user.class);
//        stream.print();
        System.out.println(table.getQueryOperation().getResolvedSchema().getColumnCount());
        table.printSchema();


    }
}

class user{
    private String name;
    private int score;
}
