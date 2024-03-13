package app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import utils.DimUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction2 extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//        connection.setAutoCommit(true);

        Class.forName("com.mysql.jdbc.Driver");  // 加载数据库驱动
        connection = DriverManager.getConnection(  // 获取连接
                "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8&useSSL=false",  // 数据库URL
                "root",  // 用户名
                "123456");  // 登录密码
        connection.setAutoCommit(true);
    }

    //value:{"sinkTable":"dim_base_trademark","database":"gmall-210325-flink","before":{"tm_name":"atguigu","id":12},"after":{"tm_name":"Atguigu","id":12},"type":"update","tableName":"base_trademark"}
    //SQL：upsert into db.tn(id,tm_name) values('...','...')
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String type = value.getString("type");

            String date = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
                    .format(Calendar.getInstance().getTime());

            if ("update".equals(type)) {
                /**
                 * 如果是更新
                 *      1.获取指定字段(java bean)
                 *      2.判断hash值(！= null)
                 */
                String updateSql = getUpdateSql(sinkTable, after, date);
                preparedStatement = connection.prepareStatement(updateSql);
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
                preparedStatement.executeUpdate();
                String isnertSql = getInsertSql(sinkTable,after,date);
                preparedStatement = connection.prepareStatement(isnertSql);
                preparedStatement.executeUpdate();
            } else if ("insert".equals(type)) {
                /**
                 * 如果是新增：   join其他维度，  insert到输出表
                 */
                String isnertSql = getInsertSql(sinkTable, after, date);
                preparedStatement = connection.prepareStatement(isnertSql);
                preparedStatement.executeUpdate();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    private String getUpdateSql(String sinkTable, JSONObject data, String endTime) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        //keySet.mkString(",");  =>  "id,tm_name"
        String next = keySet.iterator().next();
        return "UPDATE test02." + sinkTable + " SET  endTime = '" + endTime + "' WHERE name = '" + data.getString("name") + "'";

//        return "INSERT INTO test02." + sinkTable + "(" +
//                StringUtils.join(keySet, ",") + ") VALUES ('" +
//                StringUtils.join(values, "','") + "') " +
//                "ON DUPLICATE KEY UPDATE " +
//                next + "=VALUES(" + next +")";
    }

    private String getInsertSql(String sinkTable, JSONObject data, String startTime) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        //keySet.mkString(",");  =>  "id,tm_name"
        String next = keySet.iterator().next();
        return "INSERT INTO test02." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ",startTime,endTime" + ") VALUES ('" +
                StringUtils.join(values, "','") + "','" + startTime + "','9999-99-99 99:99:99') ";
    }

    //data:{"tm_name":"Atguigu","id":12}
    //SQL：upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        //keySet.mkString(",");  =>  "id,tm_name"
        String next = keySet.iterator().next();
        return "INSERT INTO test02." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") VALUES ('" +
                StringUtils.join(values, "','") + "') " +
                "ON DUPLICATE KEY UPDATE " +
                next + "=VALUES(" + next + ")";
    }
}
