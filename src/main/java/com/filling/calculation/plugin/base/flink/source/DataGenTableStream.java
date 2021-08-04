package com.filling.calculation.plugin.base.flink.source;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.filling.calculation.common.CheckConfigUtil;
import com.filling.calculation.common.CheckResult;
import com.filling.calculation.common.PropertiesUtil;
import com.filling.calculation.common.TypesafeConfigUtils;
import com.filling.calculation.domain.DataGenField;
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
import scala.Int;

import java.util.*;

public class DataGenTableStream implements FlinkStreamSource<Row> {

    private JSONObject config;

    private Object schemaInfo;

    private static final String SCHEMA = "schema";

    private static Integer rowsPerSecond;
    private static Long numberOfRows;

    private static List<Map<String, DataGenField>> fields;

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

        CheckResult result = CheckConfigUtil.check(config, SCHEMA, RESULT_TABLE_NAME, "fields");

        return result;
    }

    @Override
    public void prepare(FlinkEnvironment env) {

        schemaInfo = JSONObject.parse(config.getString(SCHEMA), Feature.OrderedField);
        rowsPerSecond = config.getInteger("rows-per-second");
        numberOfRows = config.getLong("number-of-rows");

        fields = JSONObject.parseObject(config.getString("fields"), List.class);
    }

    @Override
    public DataStream<Row> getStreamData(FlinkEnvironment env) {

        TypeInformation<Row> typeInfo = SchemaUtil.getTypeInformation((JSONObject) schemaInfo);

        RandomDataGenInput randomDataGenInput    = new RandomDataGenInput(fields);
        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(randomDataGenInput, rowsPerSecond, numberOfRows);

        DataStream dataStream = env.getStreamExecutionEnvironment().addSource(dataGeneratorSource).returns(typeInfo).name(getName()).setParallelism(getParallelism());

        return dataStream;
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
