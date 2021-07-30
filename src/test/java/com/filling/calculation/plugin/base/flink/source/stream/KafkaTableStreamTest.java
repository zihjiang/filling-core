package com.filling.calculation.plugin.base.flink.source.stream;

import com.filling.calculation.Waterdrop;
import com.filling.calculation.domain.RunModel;
import com.filling.calculation.flink.util.Engine;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class KafkaTableStreamTest {

    private String configPath;

    private String rootPath;

    @Before
    public void setup() {

        rootPath = this.getClass().getResource("/").getPath();
    }

    @Test
    public void testKafka2es() throws Exception {
        configPath = "flink/kafka2es.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Waterdrop.entryPoint(str, Engine.FLINK, RunModel.PROD);
    }

    @Test
    public void testKafkaJoinJdbc2es() throws Exception {
        configPath = "flink/KafkaJoinJdbc2es.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Waterdrop.entryPoint(str, Engine.FLINK, RunModel.PROD);
    }

    @Test
    public void testKafkaDataAggregates() throws Exception {

        configPath = "flink/KafkaDataAggregates.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Waterdrop.entryPoint(str, Engine.FLINK, RunModel.PROD);
    }

    @Test
    public void testKafkaDataAggregates4pf() throws Exception {

        configPath = "flink/KafkaDataAggregates4pf.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Waterdrop.entryPoint(str, Engine.FLINK, RunModel.PROD);
    }

    @Test
    public void testkafka2esAuth() throws Exception {

        configPath = "flink/kafka2esAuth.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Waterdrop.entryPoint(str, Engine.FLINK, RunModel.DEV);
    }

    @Test
    public void testkafka2Console() throws Exception {

        configPath = "flink/kafka2Console.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Waterdrop.entryPoint(str, Engine.FLINK, RunModel.PROD);
    }

    @Test
    public void testkafk2ck() throws Exception {

        configPath = "flink/kafka2ck.json";
        String str = Files.lines(Paths.get(rootPath + configPath), StandardCharsets.UTF_8).collect(Collectors.joining());

        Waterdrop.entryPoint(str, Engine.FLINK, RunModel.PROD);
    }

}
