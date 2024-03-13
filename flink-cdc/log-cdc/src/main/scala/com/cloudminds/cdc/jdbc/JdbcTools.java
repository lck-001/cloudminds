package com.cloudminds.cdc.jdbc;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class JdbcTools {
    private static final Logger logger = LoggerFactory.getLogger(JdbcTools.class);
    private String type = null;

    private String db = null;

    private String tableName = null;

    private String url = null;

    private String username = null;

    private String password = null;


    public JdbcTools(String type,String db,String tableName,String url,String username,String password){
        this.type=type;
        this.db=db;
        this.tableName=tableName;
        this.url=url;
        this.username=username;
        this.password=password;
    }

    public String concatSql(){
        String sql = "";
        tableName = tableName.replaceAll("`", "");
        db = db.replaceAll("`", "");
        if ("postgres".equalsIgnoreCase(type)){
            sql = "select a.attname AS \"COLUMN_NAME\",\n" +
                    "concat_ws('',t.typname,SUBSTRING(format_type(a.atttypid,a.atttypmod) from '\\(.*\\)')) as \"字段类型\"\n" +
                    "from pg_attribute a\n" +
                    "left join pg_description d on d.objoid=a.attrelid and d.objsubid=a.attnum\n" +
                    "left join pg_class c on a.attrelid = c.oid \n" +
                    "left join pg_type t on a.atttypid = t.oid \n" +
                    "where a.attnum>=0 and c.relname = '" +tableName+"' \n"+
                    "ORDER BY c.relname DESC,a.attnum ASC";
        }else if ("mysql".equalsIgnoreCase(type)){
            sql = "SELECT\n" +
                    "  COLUMN_NAME ,\n" +
                    "  DATA_TYPE \n" +
                    "FROM\n" +
                    " INFORMATION_SCHEMA.COLUMNS\n" +
                    "where\n" +
                    "-- developerclub为数据库名称，到时候只需要修改成你要导出表结构的数据库即可\n" +
                    "table_schema ='" + db +"' \n"+
                    "AND\n" +
                    "-- article为表名，到时候换成你要导出的表的名称\n" +
                    "-- 如果不写的话，默认会查询出所有表中的数据，这样可能就分不清到底哪些字段是哪张表中的了，所以还是建议写上要导出的名名称\n" +
                    "table_name  = '"+tableName+"'";
        }else{
            logger.info("暂时没有需要同步的数据类型schema");
        }
        return sql;
    }

    public Schema executeSql(String excludeColumns) {
        Connection conn = null;
        Schema schema = null;
        try {
            conn = getConnection();
            Statement statement = conn.createStatement();
            ResultSet rs = null;
            rs = statement.executeQuery(concatSql());
//            List<Map<String,Object>> results=new ArrayList<Map<String,Object>>();
            List<Map<String,Object>> schemaResults=new ArrayList<Map<String,Object>>();
            ResultSetMetaData metaData = rs.getMetaData(); // 获得列的结果
            List<String> colNameList=new ArrayList<String>();
            int cols_len = metaData.getColumnCount(); // 获取总的列数
//            System.err.println("--------------------------------------------");
            logger.info("--------------------------------------------");
            for (int i = 0; i < cols_len; i++) {
                String columnLabel = metaData.getColumnLabel(i + 1);
                String col_name = metaData.getColumnName(i + 1); // 获取第 i列的字段名称
                String col_Tpye = metaData.getColumnTypeName(i+1);//类型
                colNameList.add(metaData.getColumnName(i+1));
//                System.err.println("列名："+col_name+"类型："+col_Tpye);
                logger.info("列名："+col_name+"类型："+col_Tpye);
            }
            Map<String, Object> columnMap=new HashMap<String, Object>();
            // 添加额外属性字段
            ArrayList<String> arr = new ArrayList<>(Arrays.asList("db", "table", "bigdata_method", "event_time", "k8s_env_name"));
            // 需要排除的字段
            ArrayList<String> excludeArr = new ArrayList<>(Arrays.asList(excludeColumns.split(",")));
            while (rs.next()) {

                Map<String, Object> filedMap=new HashMap<String, Object>();

                for(int i=0;i<cols_len;i++){
                    String key=colNameList.get(i);

                    Object value=rs.getString(colNameList.get(i));
                    String colKey = "";
                    String colValue = "";
                    if ("COLUMN_NAME".equalsIgnoreCase(colNameList.get(i))){
                        arr.remove(value);
                        if (excludeArr.contains(value)){
                            break;
                        }
                        filedMap.put("name",value);
                        colKey = value.toString();
                    }else {
                        String type = ColumnType.convertColumnType(value.toString());
                        String[] tp = {type,"null"};
                        filedMap.put("type",tp);
                         colValue = type;
                    }
                    columnMap.put(colKey,colValue);
                }
//                results.add(map);
                schemaResults.add(filedMap);
            }
//            String[] arr={"db","table","bigdata_method","event_time","k8s_env_name"};
            for (String name : arr){
                if (!columnMap.containsKey(name)){
                    HashMap<String, Object> extMap = new HashMap<>();
                    extMap.put("name",name);
                    if ("event_time".equalsIgnoreCase(name)){
                        extMap.put("type","long");
                    }else{
                        extMap.put("type","string");
                    }
                    schemaResults.add(extMap);
                }
            }

            schema = createSchema(schemaResults, tableName);
//            String str = JSON.toJSON(results).toString();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return schema;
    }

    private Connection getConnection () {
        Connection conn = null;
        try{
            if ("mysql".equalsIgnoreCase(type)){
                Class.forName("com.mysql.cj.jdbc.Driver");
            }else if ("postgres".equalsIgnoreCase(type)){
                Class.forName("org.postgresql.Driver");
            }else{
                throw new Exception("no driver found !!!");
            }
            // 2. 连接数据库，返回连接对象
            conn = DriverManager.getConnection(url, username, password);
        }catch(Exception e) {
            e.printStackTrace();
        }/*finally {
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }*/
        return conn;
    }

    public Schema createSchema(List<Map<String,Object>> fileds,String tableName){
        JSONObject json = new JSONObject();
        json.put("type","record");
        json.put("name",tableName);
        json.put("fields",fileds.toArray());
        Schema schema = new Schema.Parser().parse(json.toString());
        logger.info("schema --------> "+schema);
        return schema;
    }

    public static void main(String[] args) {
        JdbcTools jdbcTools = new JdbcTools("postgres", "cdmdq", "dwd_data_check", "jdbc:postgresql://172.16.31.1:32086/cdmdq", "postgres", "cloud1688");
        jdbcTools.executeSql("");
    }

}
