package com.filling.calculation.plugin.base.flink.source;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.types.Row;

public class RandomDataGenInput implements DataGenerator<Row> {
    // 随机数据生成器对象
    RandomDataGenerator generator;
    @Override
    public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
        // 实例化生成器对象
        generator = new RandomDataGenerator();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Row next() {
        Row row = new Row(5);

        // {"host":"192.168.1.103","source":"datasource","MetricsName":"cpu","value":"49","_time":1626571020000}
        row.setField("host", generator.nextHexString(3));
        row.setField("source", generator.nextHexString(2));
        row.setField("MetricsName", generator.nextHexString(5));
        row.setField("value", generator.nextHexString(5));
        row.setField("_time", System.currentTimeMillis());
        return row;
    }
}
