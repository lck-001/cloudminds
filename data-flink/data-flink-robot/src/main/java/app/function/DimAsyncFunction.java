package app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import utils.DimUtil;
import utils.ThreadPoolUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        Class.forName("com.mysql.jdbc.Driver");  // 加载数据库驱动
        connection = DriverManager.getConnection(  // 获取连接
                "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2B8&useSSL=false",  // 数据库URL
                "root",  // 用户名
                "123456");  // 登录密码
        connection.setAutoCommit(true);


        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取查询的主键
                    String id = getKey(input);

                    //查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                    //补充维度信息
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }

                    //将数据输出
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
