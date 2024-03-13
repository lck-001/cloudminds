package org.apache.flink.table.connector.sourceT;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fusesource.mqtt.client.*;
import org.apache.flink.table.data.RowData;


public class MqttSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
//public class MqttSourceFunction extends TableFunction<RowData> {
    //    private final String hosturl;
//    private final String username;
//    private final String password;
//    private final String topic;
//    private final DeserializationSchema<RowData> deserializer;
//    public MqttSourceFunction(String hosturl, String username, String password,String topic) {
//        this.hosturl = hosturl;
//        this.username = username;
//        this.password = password;
//        this.topic = topic;
//        this.deserializer = deserializer;
//    }
    private final DeserializationSchema<RowData> deserializer;
    private BlockingConnection blockingConnection;
    private ReadableConfig options;

    public MqttSourceFunction(ReadableConfig options,DeserializationSchema<RowData> deserializer) {
        this.options = options;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
//        return TypeInformation.of(RowData.class);
    }
    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        System.out.println("[Source] Running source");
        MQTT mqtt = new MQTT();
        mqtt.setHost(options.get(MqttDynamicTableSourceFactory.HOSTURL));
        mqtt.setUserName(options.get(MqttDynamicTableSourceFactory.USERNAME));
        mqtt.setPassword(options.get(MqttDynamicTableSourceFactory.PASSWORD));
        mqtt.setWillTopic(options.get(MqttDynamicTableSourceFactory.TOPIC));
        blockingConnection = mqtt.blockingConnection();
        blockingConnection.connect();
        System.out.println("[Source] Connected to mqtt broker");
        System.out.println("[Source] Subscribed to " + options.get(MqttDynamicTableSourceFactory.TOPIC));

        while(blockingConnection.isConnected()) {
            Message message = blockingConnection.receive();
            //获取到的数据
            String topic = message.getTopic();
            byte[] payload = message.getPayload();
            message.ack();
            sourceContext.collect(deserializer.deserialize(payload));
        }
        blockingConnection.disconnect();
    }


    @Override
    public void cancel() {
        try {
            blockingConnection.disconnect();
        } catch (Throwable t) {
            // ignore
        }
    }
}
