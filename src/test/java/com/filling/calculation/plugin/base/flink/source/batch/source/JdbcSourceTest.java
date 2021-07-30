package com.filling.calculation.plugin.base.flink.source.batch.source;

import com.alibaba.fastjson.JSONObject;
import com.filling.calculation.Waterdrop;
import com.filling.calculation.domain.PreviewResult;
import com.filling.calculation.domain.RunModel;
import com.filling.calculation.flink.util.Engine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @program: calculation-core
 * @description:
 * @author: zihjiang
 * @create: 2021-06-26 16:10
 **/
public class JdbcSourceTest {


    private String configPath;

    private String rootPath;

    @Before
    public void setup() {

        rootPath = this.getClass().getResource("/").getPath();
    }

    @Test
    public void testSourceJdbc() throws Exception {
        configPath = "flink/batch/BatchJdbc.json";
        String inputConfig = readFile(configPath);
        String outputResult = readFile("flink/batch/result/JdbcSourceBatch.json");

        List<PreviewResult> list = Waterdrop.entryPoint(inputConfig, Engine.FLINK, RunModel.DEV);
        Assert.assertEquals(outputResult, JSONObject.toJSONString(list));
    }


    private String readFile(String path) {
        String result = "";
        try {
            result = Files.lines(Paths.get(rootPath + path), StandardCharsets.UTF_8).map(s -> s.replaceAll(" ", "")).collect(Collectors.joining());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }


}
