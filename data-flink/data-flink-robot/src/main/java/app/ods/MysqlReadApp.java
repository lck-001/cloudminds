package app.ods;

import java.sql.*;

public class MysqlReadApp {
    static String tableName = "roc.t_user_rcu_robot";
    static String sql = "select count(*) from " + tableName;//SQL语句    输入表名information_test
    static ResultSet rs = null;
    public static final String url = "jdbc:mysql://172.16.31.1:31541/roc?serverTimezone=GMT%2B8&useSSL=false"; // 数据库URL
    public static final String user = "bigdata_sync_r";//用户名
    public static final String password = "bigdata_sync_r";//密码
    public static Connection conn = null;
    public static PreparedStatement ps = null;
    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");//指定连接类型
            conn = DriverManager.getConnection(url, user, password);//获取连接
            ps = conn.prepareStatement(sql);//准备执行语句
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        //显示数据
        try {
            rs = ps.executeQuery();//执行语句

            while (rs.next()) {
                int row = rs.getRow();
                System.out.println(row);
//                int co2 = rs.getInt("CO2");//输入要输出的元素名
//                int co=rs.getInt("CO");
//                System.out.println("CO2:"+co2+"  CO:"+co);
            }
            //关闭连接
            rs.close();
            conn.close();
            ps.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

}