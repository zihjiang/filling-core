package com.filling.calculation.plugin.base.flink.source;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.filling.calculation.common.CheckConfigUtil;
import com.filling.calculation.common.CheckResult;
import com.filling.calculation.common.PropertiesUtil;
import com.filling.calculation.common.TypesafeConfigUtils;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.stream.FlinkStreamSource;
import com.filling.calculation.flink.util.SchemaUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.factories.datagen.types.RowDataGenerator;
import org.apache.flink.types.Row;

import java.util.Properties;

public class DataGenTableStream implements FlinkStreamSource<Row> {

    private JSONObject config;

    private Object schemaInfo;
    private final String consumerPrefix = "consumer.";

    private static final String SCHEMA = "schema";

    @Override
    public void setConfig(JSONObject config) {
        this.config = config;
    }

    @Override
    public JSONObject getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {

        CheckResult result = CheckConfigUtil.check(config, SCHEMA, RESULT_TABLE_NAME);

        if (result.isSuccess()) {
            JSONObject consumerConfig = TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false);
            return CheckConfigUtil.check(consumerConfig);
        }

        return result;
    }

    @Override
    public void prepare(FlinkEnvironment env) {

    }

    @Override
    public DataStream<Row> getStreamData(FlinkEnvironment env) {

        String sql = "CREATE TABLE source_table (\n" +
                "                id INT,\n" +
                "                host String,\n" +
                "                source String,\n" +
                "                MetricsName String,\n" +
                "                `value` INT,\n" +
                "                _time AS localtimestamp\n" +
                ") WITH (\n" +
                "                'connector' = 'datagen',\n" +
                "                'rows-per-second'='500000',\n" +
                "\n" +
                "                'fields.id.kind'='sequence',\n" +
                "                'fields.id.start'='1',\n" +
                "                'fields.id.end'='1000000',\n" +
                "\n" +
                "                'fields.host.kind'='random',\n" +
                "                'fields.host.length'='5',\n" +
                "\n" +
                "                'fields.source.kind'='random',\n" +
                "                'fields.source.length'='5',\n" +
                "\n" +
                "                'fields.MetricsName.kind'='random',\n" +
                "                'fields.MetricsName.length'='5',\n" +
                "\n" +
                "                'fields.value.kind'='random',\n" +
                "                'fields.value.min'='1',\n" +
                "                'fields.value.max'='100'\n" +
                "\n" +
                "        )";

        System.out.println(sql);

        env.getStreamTableEnvironment().executeSql(sql);


        Table table = env.getStreamTableEnvironment().from("source_table");

        DataStream dataStream = env.getStreamTableEnvironment().toDataStream(table);

        return dataStream;
    }

    private String[] getSchema() {
        DeserializationSchema result = null;
        String schemaContent = config.getString(SCHEMA);
        schemaInfo = JSONObject.parse(schemaContent, Feature.OrderedField);

        return new String[]{"host", "source", "MetricsName", "value"};
    }

    @Override
    public Integer getParallelism() {

        // 默认为1,
        return config.getInteger("parallelism") == null ? 1 : config.getInteger("parallelism");
    }

    @Override
    public String getName() {

        return StringUtils.isEmpty(config.getString("name")) ? config.getString("plugin_name") : config.getString("name");
    }


}
