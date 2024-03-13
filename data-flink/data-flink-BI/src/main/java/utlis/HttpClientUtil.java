package utlis;

import com.alibaba.fastjson.JSONObject;
import conf.RequestConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClientUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);
    private static HttpURLConnection con = null;
    private static BufferedReader in = null;

    private static HttpURLConnection getHttpURLConnection(String getUrl) throws IOException {
        URL url = new URL(getUrl);
        con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("charset", "utf-8");
        con.setRequestProperty("X-AUTH", RequestConf.AUTH);

        return con;
    }


    public static String httpGet(String getUrl, String params) throws IOException {

        StringBuilder inputString = new StringBuilder();
        con = HttpClientUtil.getHttpURLConnection(getUrl);


        try {
            if (null != params && !params.isEmpty()) {
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
                writer.write(params);
                writer.flush();
            }

            // 建立实际的连接
            con.connect();

            // 获取服务端响应，通过输入流来读取URL的响应
            InputStream is = con.getInputStream();
            in = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            StringBuffer sbf = new StringBuffer();
            String strRead = null;
            while ((strRead = in.readLine()) != null) {
                inputString.append(strRead);
                inputString.append("\r\n");
            }
            in.close();
            // 关闭连接
            con.disconnect();
        } catch (Exception var16) {
            LOGGER.warn("httpget threw: ", var16);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (con != null) {
                    con.disconnect();
                }
            } catch (Exception var15) {
                LOGGER.warn("httpget finally block threw: ", var15);
            }
        }
        return inputString.toString();
    }


    public static boolean updateToken(JSONObject params) throws IOException {
        //10200383--token到期；3401--token为null（重新获取token）
        String message = httpGet(RequestConf.TOKEN_URL, params.toJSONString());
        JSONObject jsonObject = JSONObject.parseObject(message);
        if (jsonObject.getInteger("code") == 200) {
            RequestConf.AUTH = jsonObject.getJSONObject("data").getString("token");
            return true;
        }else {
            return false;
        }
    }
}
