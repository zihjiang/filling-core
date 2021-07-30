package com.filling.calculation.plugin.base.flink.sink;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.types.Row;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;


public class ClickHouseOutputFormat<T> extends RichOutputFormat<T> implements SinkFunction {

    private JSONObject config;

    private final static String PREFIX = "es.";

    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private int batchSize = 5000;

    JdbcOutputFormat jdbcOutputFormat;


    public ClickHouseOutputFormat(JSONObject userConfig) {
        this.config = userConfig;
    }

    @Override
    public void configure(Configuration configuration) {
        System.out.println("--------configure");

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



        jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setBatchSize(batchSize)
                .setQuery(query)
                .finish();
    }

    @Override
    public void open(int i, int i1) throws IOException {
        System.out.println("--------configure");

        jdbcOutputFormat.open(1,1);
    }

    @Override
    public void writeRecord(T t) throws IOException {

        jdbcOutputFormat.writeRecord((Row) t);
    }

    @Override
    public void close() {
    }

}
