package com.filling.calculation.plugin.base.flink.sink;

import com.alibaba.fastjson.JSONObject;

import com.filling.calculation.common.CheckConfigUtil;
import com.filling.calculation.common.CheckResult;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.batch.FlinkBatchSink;
import com.filling.calculation.flink.stream.FlinkStreamSink;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.io.IOException;


public class ClickHouseSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    JSONObject config;

    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private int batchSize = 5000;



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
        return CheckConfigUtil.check(config,"driver","url","query");
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

    @Override
    public void prepare(FlinkEnvironment env) {

        driverName = config.getString("driver");
        dbUrl = config.getString("url");
        username = config.getString("username");
        query = config.getString("query");
        if (config.containsKey("password")) {
            password = config.getString("password");
        }
        if (config.containsKey("batch_size")) {
            batchSize = config.getInteger("batch_size");
        }
    }


    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {

        return dataStream.addSink(
                JdbcSink.sink(
                        query
                ,
                (statement, row) -> {
//                    statement.setObject(1, row.getField("_time"));
                    statement.setObject(1, row.getField("host"));
                    statement.setObject(2, row.getField("MetricsName"));
                    statement.setObject(3, row.getField("value"));
                    statement.setObject(4, row.getField("host"));
                    statement.setObject(5, row.getField("source"));
                    statement.setObject(6, row.getField("_time"));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withDriverName(driverName)
                        .withUsername(username)
                        .withPassword(password)
                        .build())
        ).setParallelism(getParallelism()).name(getName());

    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        final Table table = env.getBatchTableEnvironment().fromDataSet(dataSet);
//        createSink(env.getBatchTableEnvironment(),table);
        dataSet.output(new ClickHouseOutputFormat<>(config));
        return null;
    }

}
