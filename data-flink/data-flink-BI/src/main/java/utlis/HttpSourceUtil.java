package utlis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;

import static utlis.HttpClientUtil.httpGet;
import static utlis.HttpClientUtil.updateToken;

public class HttpSourceUtil extends RichSourceFunction<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSourceUtil.class);
    private static String qurey_url;
    private static HttpURLConnection con = null;
    private static BufferedReader in = null;
    private static boolean isRunning = true;
    private static int page_num = 1;
    private static JSONObject params;

    public HttpSourceUtil(String url) {
        qurey_url = url;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        JSONObject params = new JSONObject();
        params.put("page", RequestConf.PAGE_NUM);
        params.put("size", RequestConf.PAGE_SIZE);
        params.put("updatedAtStart", RequestConf.UPDATE_FROM);
        params.put("updatedAtEnd", RequestConf.UPDATE_TO);
        params.put("appKey", RequestConf.APP_KEY);
        params.put("appSecret", RequestConf.APP_SECRET);
        params.put("factoryNumber", RequestConf.FACTORY_NUMBER);

        this.params = params;

    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            //查询列表
            String dataStr = httpGet(qurey_url, params.toJSONString());
            if (200 == JSON.parseObject(dataStr).getInteger("code")) {
                //如果获取到数据
//                try {
                JSONArray jsonArray = null;
                if ("https://v3-ali.blacklake.cn/api/resource/domain/web/v1/resources/screen/list".equals(qurey_url)) {
                    jsonArray = JSON.parseObject(dataStr).getJSONObject("data").getJSONObject("data").getJSONArray("list");
                } else {
                    jsonArray = JSON.parseObject(dataStr).getJSONObject("data").getJSONArray("list");
                }
                int list_size = jsonArray.size();
                if (list_size == 0) {
                    //判断当前数据处理完成，修改时间参数，休眠
                    Long updatedAtEnd = params.getLong("updatedAtEnd");

                    Long updatedAtToTimestamp = System.currentTimeMillis();

                    params.put("updatedAtStart", updatedAtEnd);
                    params.put("updatedAtEnd", updatedAtToTimestamp);
                    params.put("page", 1);
                    Thread.sleep(RequestConf.REQUEST_INTERVAL);
                } else {
                    params.put("page", ++page_num);
                    sourceContext.collect(dataStr);
                }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    LOGGER.info("error_page_number: " + page_num);
//                    params.put("page", ++page_num);
//                }
            } else if (JSON.parseObject(dataStr).getInteger("code") == 10200383 || 3401 == JSON.parseObject(dataStr).getInteger("code")) {
                //10200383--token到期；3401--token为null（重新获取token）
                boolean b = updateToken(params);
                if (b == false) isRunning = false;
//                Thread.sleep(60*1000);//如果多次获取，每间隔一分钟重新获取

            }
        }
    }

    @Override
    public void cancel() {
        while (!isRunning) {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (con != null) {
                con.disconnect();
            }
        }
    }
}