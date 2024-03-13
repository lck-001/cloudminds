package app;

import bean.NatsData;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nats.client.Connection;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class NatsPubApp {
    public static void main(String[] args) throws InterruptedException {
        while (true){
            try {
//                Connection nc = Nats.connect("nats://172.16.23.85:32209");
                Connection nc = Nats.connect("nats://172.16.23.85:30243");
                NatsData natsData = new NatsData();
                natsData.guid = "BBD834703F114F7DB7A5A0CDAB04A082";
                natsData.robotid = "355929099980253";
                natsData.tenantid = "86gingerlite";
                natsData.mapid = "testmap";
                natsData.mapping3d_x = 0;
                natsData.mapping3d_y = 0;
                natsData.mapping3d_z = 0;
                natsData.mapping3d_angle = 0;
                natsData.ue_loc_x = 0;
                natsData.ue_loc_y = 0;
                natsData.ue_loc_z = 0;
                natsData.ue_rotate_pitch = 0;
                natsData.ue_rotate_roll = 0;
                natsData.ue_rotate_yaw = 0;
                natsData.timestamp = System.currentTimeMillis();

                //TODO json 数据传输
//                byte[] bymsg = JSON.toJSONString(natsData).getBytes(StandardCharsets.UTF_8);
//                System.out.println("nats 发送数据： "+bymsg);
//                nc.publish("ck",bymsg);

                //TODO gson 数据传输
                // use Gson to encode the object to JSON
                GsonBuilder builder = new GsonBuilder();
                Gson gson = builder.create();
                String json = gson.toJson(natsData);
                // Publish the message
                nc.publish("ck_test", json.getBytes(StandardCharsets.UTF_8));
                // Make sure the message goes through before we close
                nc.flush(Duration.ZERO);
                nc.close();
                System.out.println("pub success");
            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(5);
        }

    }
}
