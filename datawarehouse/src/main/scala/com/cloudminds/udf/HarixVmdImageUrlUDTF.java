package com.cloudminds.udf;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class HarixVmdImageUrlUDTF extends GenericUDTF {

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
        fieldNames.add("image_url");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("takes only one argument");
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("image_url");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }


    @Override
    public void process(Object[] deferredObjects) throws HiveException {
        {
            Set<String> response=new HashSet<>();
            if (deferredObjects == null) {
                throw new UDFArgumentException("The operator 'HarixVmdImageUrlUdf' accepts 1 argument.");
            }
            //参数校验
            if (deferredObjects[0] == null) {
                throw new UDFArgumentException("参数不能为空");
            }
            String request = deferredObjects[0].toString();
            if (StringUtils.isEmpty(request)) {
                throw new UDFArgumentException("参数值不能为空");
            }
            //解析request里的图片地址
            JSONObject jsonObject=JSONObject.parseObject(request);
            if(jsonObject==null){
                return;
            }
            if(jsonObject.getJSONObject("request")!=null&&jsonObject.getJSONObject("request").getJSONObject("params")!=null) {
                String requestImageUrl = jsonObject.getJSONObject("request").getJSONObject("params").getString("image_url");
                if (!StringUtils.isEmpty(requestImageUrl)) {
                    response.add(requestImageUrl);
                }
            }
            //解析detail里的图片地址
            JSONObject detailJsonObject=jsonObject.getJSONObject("detail");
            if(detailJsonObject!=null){
                for(String detailKey:detailJsonObject.keySet()){
                    if(detailKey.contains("imagePath")){
                        String detailImageUrl=detailJsonObject.getString(detailKey);
                        if(!StringUtils.isEmpty(detailImageUrl)){
                            response.add(detailImageUrl);
                        }
                    }
                }
            }
          for(String imageUrl:response) {
              String[] result=new String[1];
              result[0]=imageUrl;
              forward(result);
          }
        }
    }

    @Override
    public void close() throws HiveException {

    }

}
