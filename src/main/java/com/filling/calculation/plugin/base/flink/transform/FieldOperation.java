package com.filling.calculation.plugin.base.flink.transform;


import com.filling.calculation.common.CheckConfigUtil;
import com.filling.calculation.common.CheckResult;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.batch.FlinkBatchTransform;
import com.filling.calculation.flink.stream.FlinkStreamTransform;
import com.filling.calculation.flink.util.TableUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.types.Row;

import java.util.List;

public class FieldOperation implements FlinkBatchTransform<Row, Row> , FlinkStreamTransform<Row, Row> {


    private JSONObject config;

    private static String SCRIPT_NAME = "script";
    private static String TARGET_FIELD_NAME = "target_field";
    private static String SCRIPT = null;

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {

        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();

        return (DataStream<Row>) process(tableEnvironment, dataStream, "stream");
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();

        return (DataSet<Row>) process(tableEnvironment, data, "batch");
    }

    private Object process(TableEnvironment tableEnvironment, Object data, String type) {

        String sql = "select *,({script}) as {target_field_name} from {table_name}"
            .replaceAll("\\{script}", config.getString(SCRIPT_NAME))
            .replaceAll("\\{target_field_name}", config.getString(TARGET_FIELD_NAME))
            .replaceAll("\\{table_name}", config.getString(SOURCE_TABLE_NAME));

//        Table table = tableEnvironment.from(config.getString(SOURCE_TABLE_NAME)).addOrReplaceColumns(config.getString(SCRIPT_NAME)).as(config.getString(TARGET_FIELD_NAME));

        Table table = tableEnvironment.sqlQuery(sql);
        return "batch".equals(type) ? TableUtil.tableToDataSet((BatchTableEnvironment) tableEnvironment, table) : TableUtil.tableToDataStream((StreamTableEnvironment) tableEnvironment, table, false);
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
       return CheckConfigUtil.check(config,SCRIPT_NAME );
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        SCRIPT = config.getString(SCRIPT_NAME);
    }

    /**
     * 防止idea提示废弃
     * @param expr
     * @return
     */
    private Expression[] _(String expr) {

        List<Expression> expressions = ExpressionParser.parseExpressionList(expr);
        return expressions.toArray(new Expression[0]);
    }
}
