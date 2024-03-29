package org.apache.flink.table.connector.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class MqttDynamicTableSource implements ScanTableSource {
    private ReadableConfig options;
    private TableSchema schema;
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    public MqttDynamicTableSource(ReadableConfig options, DecodingFormat<DeserializationSchema<RowData>> decodingFormat, TableSchema schema){
        this.options = options;
        this.decodingFormat = decodingFormat;
        this.schema = schema;
    }

    @Override
    //写入方式默认INSERT_ONLY,里面实现了一个static静态类初始化
    public ChangelogMode getChangelogMode() {
//        return decodingFormat.getChangelogMode();
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    //获取运行时类
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {

        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                ctx,
                schema.toPhysicalRowDataType());
        final SourceFunction<RowData> sourceFunction = new MqttSourceFunction(options,deserializer);
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new MqttDynamicTableSource(options,decodingFormat,schema);
    }

    @Override
    public String asSummaryString() {
        return "Mqtt Table Source";
    }
}
