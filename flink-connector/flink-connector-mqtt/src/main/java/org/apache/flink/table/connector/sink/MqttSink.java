package org.apache.flink.table.connector.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.Set;

public class MqttSink implements DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }
}
