package com.filling.calculation.plugin.base.flink.source;

import com.alibaba.fastjson.JSONObject;
import com.filling.calculation.domain.DataGenField;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataGenInput implements DataGenerator<Row> {

    private List<Map<String, DataGenField>> fields;

    public DataGenInput(List<Map<String, DataGenField>> fields) {

        this.fields = fields;
    }
    // 随机数据生成器对象
    RandomDataGenerator randomDataGenerator;

    @Override
    public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
        // 实例化生成器对象
        randomDataGenerator = new RandomDataGenerator();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Row next() {
        Row row = Row.withNames();
        for (int i = 0; i < fields.size(); i++) {
            Map map = fields.get(i);
            map.keySet().stream().forEach(
                    key -> {
                        DataGenField dataGenField = JSONObject.parseObject(map.get(key).toString(), DataGenField.class);
                        switch (dataGenField.getType()){
                            case "String":
                                row.setField(key.toString(), randomDataGenerator.nextHexString(dataGenField.getLength()));
                                break;
                            case "Int":
                                row.setField(key.toString(), randomDataGenerator.nextInt(dataGenField.getMin(), dataGenField.getMax()));
                                break;
                            default:

                                break;
                        }
                    }
            );
        }

        return row;
    }
}
