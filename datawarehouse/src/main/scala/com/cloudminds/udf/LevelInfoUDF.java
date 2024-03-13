package com.cloudminds.udf;

import groovy.transform.Synchronized;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LevelInfoUDF extends GenericUDF {
    private Map<String, Map<String, String>> levelMap;
    private transient ListObjectInspector arrayOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //判断参数个数是不是5个
        if (objectInspectors.length != 5) {
            throw new UDFArgumentException("args must accept 5 args");
        }
        if ((objectInspectors[3].getCategory().equals(ObjectInspector.Category.LIST))) {
            this.arrayOI = ((ListObjectInspector) objectInspectors[3]);
        }
        //初始化变量
        levelMap = new HashMap<String, Map<String, String>>();
        return ObjectInspectorFactory.getStandardMapObjectInspector (
                PrimitiveObjectInspectorFactory
                        .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),PrimitiveObjectInspectorFactory
                        .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));
    }

    @Override
    public Map<String, String> evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects == null || deferredObjects.length != 5) {
            throw new UDFArgumentException("The operator 'LevelInfoUDF' accepts 5 arguments.");
        }
        //参数校验
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null || deferredObjects[2].get() == null || deferredObjects[4].get() == null) {
            throw new UDFArgumentException("第一个基本列名&第二个基准列名的父列名&表名&基准列的id值不能为空");
        }
        String id = deferredObjects[4].get().toString();
        if (StringUtils.isEmpty(id)) {
            throw new UDFArgumentException("第5个参数值不能为空");
        }
        if (levelMap==null||levelMap.isEmpty()) {
            getDateFromTable(deferredObjects[0].get().toString(), deferredObjects[1].get().toString(), deferredObjects[2].get().toString(), deferredObjects[3].get());
        }

        if (levelMap.containsKey(id)) {
            return levelMap.get(id);
        } else {
            throw new UDFArgumentException("此id的层级关系原始数据里面没有,请排查函数");
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return strings[0];
    }

    @Synchronized
    public void getDateFromTable(String idname, String pidName, String tableName, Object array) throws HiveException {
        if(levelMap==null){
            levelMap=new HashMap<>();
        }

        if(!levelMap.isEmpty()){
            return;
        }

        //校验传入的前三个列名不能为空
        if (StringUtils.isEmpty(idname) || StringUtils.isEmpty(pidName) || StringUtils.isEmpty(tableName)) {
            throw new UDFArgumentException("前四个参数不能为空");
        }
        //声明用来做计算层级的中间变量
        Map<String, String[]> pidMap = new HashMap<String, String[]>();
        List<String> arrayNames = new ArrayList<String>();

        //查询数据
        Connection conn = null;
        String query_sql = "select " + idname + ", " + pidName;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://bigdata1:10000/sv", "tanqiong", "123456");
            //组装查询sql,并将转换需要拼接的列
            if (array != null&this.arrayOI!=null) {
                Integer arrayLength = this.arrayOI.getListLength(array);
                if (arrayLength > 0) {
                    for (int i = 0; i < arrayLength; i++) {
                        if (this.arrayOI.getListElement(array, i) != null) {
                            String listElement = this.arrayOI.getListElement(array, i).toString();
                            if (!listElement.isEmpty()) {
                                arrayNames.add(listElement);
                                query_sql = query_sql + ", " + arrayNames.get(i);
                            }
                        }
                    }
                }
            }
            query_sql = query_sql + " from " + tableName;

            //去数据库查询
            PreparedStatement stmt = conn.prepareStatement(query_sql);
            ResultSet rs = stmt.executeQuery();
            //查询数据入map存储
            while (rs.next()) {
                String[] info = new String[arrayNames.size() + 1];
                info[0] = rs.getString(2);
                for (int i = 0; i < arrayNames.size(); i++) {
                    info[i + 1] = rs.getString(3 + i);
                }
                pidMap.put(rs.getString(1), info);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new UDFArgumentException(query_sql + "查询出错");
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
            }
        }

        //表里没数据报错出去
        if (pidMap.size() == 0) {
            throw new UDFArgumentException(tableName + "里没有数据");
        }

        //组装成需要的数据出去
        for (String id : pidMap.keySet()) {
            String[] info =pidMap.get(id).clone();
            int level = 1;
            if ((!StringUtils.isBlank(info[0]))&&(pidMap.get(info[0])!=null)) {
                level = level + 1;
                String[] fatherInfo = pidMap.get(info[0]);
                for (int i = 1; i < info.length; i++) {
                    info[i] = fatherInfo[i] + "/" + info[i];
                }
                while ((!StringUtils.isBlank(fatherInfo[0]))&&(pidMap.get(fatherInfo[0])!=null)) {
                    fatherInfo = pidMap.get(fatherInfo[0]);
                    level = level + 1;
                    for (int i = 1; i < info.length; i++) {
                        info[i] = fatherInfo[i] + "/" + info[i];
                    }
                }
            }
            Map<String, String> backInfo = new HashMap<String, String>();
            backInfo.put("level", level+"");
            for (int i = 0; i < arrayNames.size(); i++) {
                backInfo.put(arrayNames.get(i), info[1 + i]);
            }
            levelMap.put(id, backInfo);
        }

    }

}
