package com.filling.calculation.plugin.base.flink.source;

import com.filling.calculation.common.CheckConfigUtil;
import com.filling.calculation.common.CheckResult;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.batch.FlinkBatchSource;
import com.filling.calculation.flink.util.SchemaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcRowInputFormat;
import org.apache.flink.types.Row;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class CustomSource implements FlinkBatchSource<Row> {

    private JSONObject config;

    private InputFormat inputFormat;

    private static Path PATH = null;

    private final static String SIMPLE_DATA = "simple_data";
    private final static String SOURCE_FORMAT = "format.type";
    private final static String SCHEMA = "schema";

    @Override
    public DataSet<Row> getBatchData(FlinkEnvironment env) {




        return env.getBatchEnvironment().createInput(inputFormat).setParallelism(getParallelism()).name(getName());
    }


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
        // 如果schema为json, 并且没有设置schema, 则用simple_data第一条作为schema
        if("json".equals(config.getString(SOURCE_FORMAT)) && !CheckConfigUtil.check(config, SIMPLE_DATA).isSuccess() && !CheckConfigUtil.check(config, SCHEMA).isSuccess() ) {
            String simple_data = config.getString(SIMPLE_DATA);
            config.put(SCHEMA, simple_data.split("\n")[0]);
        } else if("text".equals(config.getString(SOURCE_FORMAT))) {
            return CheckConfigUtil.check(config,SOURCE_FORMAT, SIMPLE_DATA);
        }
        return CheckConfigUtil.check(config,SOURCE_FORMAT,SCHEMA, SIMPLE_DATA);

    }

    @Override
    public void prepare(FlinkEnvironment env) {
        // 先写入临时文件, 其他的处理流程和fileSource基本一致
        PATH = getTempFile();
        String format = config.getString(SOURCE_FORMAT);
        String schemaContent = config.getString(SCHEMA);
        switch (format) {
            case "json":
                Object jsonSchemaInfo = JSONObject.parse(schemaContent);
                RowTypeInfo jsonInfo = SchemaUtil.getTypeInformation((JSONObject) jsonSchemaInfo);
                JsonRowInputFormat jsonInputFormat = new JsonRowInputFormat(PATH, null, jsonInfo);
                inputFormat = jsonInputFormat;
                break;
            case "parquet":
//                final Schema parse = new Schema.Parser().parse(schemaContent);
//                final MessageType messageType = new AvroSchemaConverter().convert(parse);
//                inputFormat = new ParquetRowInputFormat(filePath, messageType);
                break;
            case "orc":
                OrcRowInputFormat orcRowInputFormat = new OrcRowInputFormat(PATH.toString(), schemaContent, null, 1000);
                this.inputFormat = orcRowInputFormat;
                break;
            case "csv":
                Object csvSchemaInfo = JSONObject.parse(schemaContent);
                TypeInformation[] csvType = SchemaUtil.getCsvType((List<Map<String, String>>) csvSchemaInfo);
                RowCsvInputFormat rowCsvInputFormat = new RowCsvInputFormat(PATH, csvType, true);
                this.inputFormat = rowCsvInputFormat;
                break;
            case "text":
                TextRowInputFormat textInputFormat = new TextRowInputFormat(PATH);
                inputFormat = textInputFormat;
                break;
            default:
                break;
        }

    }

    // 获取临时文件目录
    private Path getTempFile() {

        String tempPath =System.getProperty("java.io.tmpdir")+ File.separator + UUID.randomUUID();

        String simple_data = config.getString(SIMPLE_DATA);
        byte data[] = simple_data.getBytes();
        java.nio.file.Path p = Paths.get(tempPath);

        try (OutputStream out = new BufferedOutputStream(
            Files.newOutputStream(p ))) {
            out.write(data, 0, data.length);
        } catch (IOException x) {
            System.err.println(x);
        }
        return new Path(tempPath);
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
