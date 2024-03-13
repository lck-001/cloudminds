package utils;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class NatsUtil extends RichParallelSourceFunction<String> {

    private static Boolean isRunning = true;
    private static String topic;
    private static String url;

    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, String> stringStringMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        this.topic = stringStringMap.get("nats_topic");
        this.url = stringStringMap.get("nats_url");
    }


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int num = 1;
        while (isRunning) {
            try {
                Connection nc = Nats.connect(url);
                CountDownLatch latch = new CountDownLatch(num);
                Dispatcher d = nc.createDispatcher((msg) -> {
                    String str = new String(msg.getData(), StandardCharsets.UTF_8);
                    sourceContext.collect(str);
                    latch.countDown();
                });
                d.subscribe(topic);
                num = 1;
                latch.await();
                nc.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }
}
