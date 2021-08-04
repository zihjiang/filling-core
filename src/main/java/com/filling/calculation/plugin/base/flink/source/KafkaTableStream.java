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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.UUID;

public class KafkaTableStream implements FlinkStreamSource<Row> {

    private JSONObject config;

    private Properties kafkaParams = new Properties();
    private String topic;
    private Object schemaInfo;
    private String tableName;
    private final String consumerPrefix = "consumer.";
    private String format;

    private static final String TOPICS = "topics";
    private static final String SCHEMA = "schema";
    private static final String SOURCE_FORMAT = "format.type";
    private static final String GROUP_ID = "group.id";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String OFFSET_RESET = "offset.reset";

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

        CheckResult result = CheckConfigUtil.check(config, TOPICS, SCHEMA, SOURCE_FORMAT, RESULT_TABLE_NAME);

        if (result.isSuccess()) {
            JSONObject consumerConfig = TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false);
            return CheckConfigUtil.check(consumerConfig, BOOTSTRAP_SERVERS, GROUP_ID);
        }

        return result;
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        topic = config.getString(TOPICS);
        PropertiesUtil.setProperties(config, kafkaParams, consumerPrefix, false);
        tableName = config.getString(RESULT_TABLE_NAME);
        format = config.getString(SOURCE_FORMAT);
    }

    @Override
    public DataStream<Row> getStreamData(FlinkEnvironment env) {

        KafkaSourceBuilder kafkaSourceBuilder = KafkaSource.<Row>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(getSchema())
                .setProperties(kafkaParams)
                .setTopics(topic);

        if (config.containsKey(OFFSET_RESET)) {
            String reset = config.getString(OFFSET_RESET);
            switch (reset) {
                case "latest":
                    kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                    break;
                case "earliest":
                    kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                    break;
                default:
//                    kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets());
                    // TODO
                    break;
            }
        }

        return env.getStreamExecutionEnvironment().fromSource(kafkaSourceBuilder.build(), WatermarkStrategy.noWatermarks(), tableName).setParallelism(getParallelism()).name(getName());
    }

    private DeserializationSchema getSchema() {
        DeserializationSchema result = null;
        String schemaContent = config.getString(SCHEMA);
        schemaInfo = JSONObject.parse(schemaContent, Feature.OrderedField);
        switch (format) {
            case "csv":
                //TODO
//                new CsvRowDeserializationSchema.Builder()
            case "json":

                TypeInformation<Row> typeInfo = SchemaUtil.getTypeInformation((JSONObject) schemaInfo);
                // 忽略转换错误引发的退出任务, 提升健壮性,
                result = new JsonRowDeserializationSchema.Builder(typeInfo).ignoreParseErrors().build();
            default:
                break;
        }
        return result;
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
