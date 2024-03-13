package org.apache.flink.table.connector.sourceT;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;

public class MqttDynamicTableSource implements ScanTableSource {
//    private final String hosturl;
//    private final String username;
//    private final String password;
//    private final String topic;
//    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
//    private final DataType producedDataType;
//    public MqttDynamicTableSource(
//            String hosturl, String username, String password, String topic,
////            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
//            DataType producedDataType) {
//        this.hosturl = hosturl;
//        this.username = username;
//        this.password = password;
//        this.topic = topic;
////        this.decodingFormat = decodingFormat;
//        this.producedDataType = producedDataType;
//    }
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private ReadableConfig options;
    private TableSchema schema;
    public MqttDynamicTableSource(ReadableConfig options, TableSchema schema,DecodingFormat<DeserializationSchema<RowData>> decodingFormat){
        this.options = options;
        this.schema = schema;
        this.decodingFormat = decodingFormat;
    }

    @Override
    //写入方式默认INSERT_ONLY,里面实现了一个static静态类初始化
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
//        return ChangelogMode.all();
//        return ChangelogMode.newBuilder()
//                .addContainedKind(RowKind.INSERT)
//                .addContainedKind(RowKind.UPDATE_BEFORE)
//                .addContainedKind(RowKind.UPDATE_AFTER)
//                .addContainedKind(RowKind.DELETE)
//                .build();
    }

    @Override
    //获取运行时类
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                runtimeProviderContext,
                schema.toPhysicalRowDataType());

//        final SourceFunction<RowData> sourceFunction = new MqttSourceFunction(
//                hosturl, username, password,topic,
//                deserializer);
//        final SourceFunction<RowData> sourceFunction = new MqttSourceFunction(
//                hosturl, username, password,topic);

        final SourceFunction<RowData> sourceFunction = new MqttSourceFunction(options,deserializer);
        return SourceFunctionProvider.of(sourceFunction, false);
    }


    @Override
    public DynamicTableSource copy() {
//        return new MqttDynamicTableSource(hosturl, username, password,topic, decodingFormat, producedDataType);
//        return new MqttDynamicTableSource(hosturl, username, password,topic, producedDataType);
        return new MqttDynamicTableSource(options,schema,decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "Socket Table Source";
    }
}
