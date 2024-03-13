package com.cloudminds.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;

public class VendingVideoUrlUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector args)
            throws UDFArgumentException {
        if (args.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentLengthException("takes only one argument");
        }
        if (!args.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName().equals("string")) {
            throw new UDFArgumentException("takes string as a parameter");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("video_url");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("report_time");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("takes only one argument");
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("video_url");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("report_time");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        Map<String,Long> response=new HashMap<>();
        if (objects[0] == null) {
            return;
        }
        String request = objects[0].toString();
        if (StringUtils.isEmpty(request)) {
            return;
        }
        JSONObject jsonObject=JSONObject.parseObject(request);
        if(jsonObject==null) {
            return;
        }
        JSONArray eventReports=jsonObject.getJSONArray("eventReport");
        if(eventReports==null||eventReports.size()==0){
            return;
        }
        for(int i=0;i<eventReports.size();i++){
            JSONObject eventReport=eventReports.getJSONObject(i);
            if(eventReport!=null&&!StringUtils.isEmpty(eventReport.getString("videoUrl"))) {
                response.put(eventReport.getString("videoUrl"),eventReport.getLongValue("reportTime"));

            }
        }
        for(String key:response.keySet()){
            Object[] result=new Object[2];
            result[0]=key;
            result[1]=response.get(key);
            forward(result);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
