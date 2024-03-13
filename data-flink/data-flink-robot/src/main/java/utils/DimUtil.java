package utils;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //查询Phoenix之前先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        //DIM:DIM_USER_INFO:143
        String redisKey = "DIM:" + tableName.toUpperCase() + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        //拼接查询语句
        //select * from db.tn where id='18';
        String querySql = "select * from test." + tableName +
                " where id='" + id + "'";

        //查询Phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);

        //在返回结果之前,将数据写入Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {

//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        Class.forName("com.mysql.jdbc.Driver");  // 加载数据库驱动
        Connection connection = DriverManager.getConnection(  // 获取连接
                "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8&useSSL=false",  // 数据库URL
                "root",  // 用户名
                "123456");  // 登录密码

        System.out.println(getDimInfo(connection, "user", "1"));
//
//        connection.close();

    }
}
