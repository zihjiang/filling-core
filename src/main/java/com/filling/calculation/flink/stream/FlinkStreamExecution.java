package com.filling.calculation.flink.stream;

import com.filling.calculation.common.CheckResult;
import com.filling.calculation.domain.PreviewResult;
import com.filling.calculation.domain.RunModel;
import com.filling.calculation.env.Execution;
import com.filling.calculation.env.RuntimeEnv;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.util.TableUtil;
import com.filling.calculation.plugin.Plugin;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkStreamExecution implements Execution<FlinkStreamSource, FlinkStreamTransform, FlinkStreamSink> {

    private JSONObject config;

    private FlinkEnvironment flinkEnvironment;


    public FlinkStreamExecution(FlinkEnvironment streamEnvironment) {
        this.flinkEnvironment = streamEnvironment;
    }

    @Override
    public List<PreviewResult> start(List<FlinkStreamSource> sources, List<FlinkStreamTransform> transforms, List<FlinkStreamSink> sinks, RunModel runModel) throws Exception {
        List<DataStream> data = new ArrayList<>();

        for (FlinkStreamSource source : sources) {
            baseCheckConfig(source);
            prepare(flinkEnvironment, source);
            DataStream dataStream = source.getStreamData(flinkEnvironment);
            data.add(dataStream);
            registerResultTable(source, dataStream);
        }

        DataStream input = data.get(0);

        for (FlinkStreamTransform transform : transforms) {
            prepare(flinkEnvironment, transform);
            baseCheckConfig(transform);
            DataStream stream = fromSourceTable(transform);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            input = transform.processStream(flinkEnvironment, stream);
            registerResultTable(transform, input);
            transform.registerFunction(flinkEnvironment);
        }

        for (FlinkStreamSink sink : sinks) {
            baseCheckConfig(sink);
            prepare(flinkEnvironment, sink);
            DataStream stream = fromSourceTable(sink);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            sink.outputStream(flinkEnvironment, stream);
        }
        try {
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private void registerResultTable(Plugin plugin, DataStream dataStream) {
        JSONObject config = plugin.getConfig();
        if (config.containsKey(RESULT_TABLE_NAME)) {
            String name = config.getString(RESULT_TABLE_NAME);
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (config.containsKey("field_name")) {
                    String fieldName = config.getString("field_name");
                    tableEnvironment.createTemporaryView(name, dataStream, fieldName);
                } else {
                    tableEnvironment.createTemporaryView(name, dataStream);
                }
            }
        }
    }

    private DataStream fromSourceTable(Plugin plugin) {
        JSONObject config = plugin.getConfig();
        if (config.containsKey(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.scan(config.getString(SOURCE_TABLE_NAME));
            return TableUtil.tableToDataStream(tableEnvironment, table, true);
        }
        return null;
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
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(Void prepareEnv) {
    }

    private static void baseCheckConfig(Plugin plugin) throws Exception {
        CheckResult checkResult = plugin.checkConfig();
        if (!checkResult.isSuccess()) {
            throw  new Exception(String.format("Plugin[%s] contains invalid config, error: %s\n"
                    , plugin.getClass().getName(), checkResult.getMsg()));
        }
    }

    private static void prepare(RuntimeEnv env, Plugin plugin) {
//        System.out.println(plugin.getConfig().toJSONString());
        plugin.prepare(env);
    }
}
