package com.filling.calculation.plugin.base.flink.source.stream;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.filling.calculation.Filling;
import com.filling.calculation.domain.RunModel;
import com.filling.calculation.flink.util.Engine;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class FillingTest {

    private String configPath;

    private String rootPath;

    @Before
    public void setup() {

        rootPath = this.getClass().getResource("/").getPath();
    }

    @Test
    public void testCase01() throws Exception {
        configPath = "flink/filling.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Filling.entryPoint(str, Engine.FLINK, RunModel.PROD);
    }

    @Test
    public void testCase02() throws Exception {
//        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("10.10.14.210")
//                .port(3306)
//                .databaseList("inventory") // monitor all tables under inventory database
//                .username("root")
//                .password("123456")
//                .tableList("orders")
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .build();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);

//        env
//                .addSource(sourceFunction)
//                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
//
//        env.execute();
    }

    @Test
    public void testessocll() {
    }
}
