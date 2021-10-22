package com.filling.calculation.plugin.base.flink.source.elasticsearch;


import com.alibaba.fastjson.JSONObject;
import com.filling.calculation.common.CheckResult;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.stream.FlinkStreamSource;
import com.filling.calculation.flink.util.SchemaUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

/**
 * @author cclient
 */
public class ElasticSearchSource
          implements FlinkStreamSource<Row> {

    @Override
    public Integer getParallelism() {
        return 1;
    }

    @Override
    public String getName() {
        return "123";
    }

    @Override
    public DataStream<Row> getStreamData(FlinkEnvironment env) throws NoSuchFieldException {
        TypeInformation<Row> typeInfo = SchemaUtil.getTypeInformation( JSONObject.parseObject("{\"name\": \"123\"}"));

        ElasticSearchInput elasticSearchInput = new ElasticSearchInput();

        DataStream dataStream = env.getStreamExecutionEnvironment().addSource(elasticSearchInput).returns(typeInfo).name(getName()).setParallelism(getParallelism());
        return dataStream;
    }

    @Override
    public void setConfig(JSONObject config) {

    }

    @Override
    public JSONObject getConfig() {
        return null;
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {

    }
}
