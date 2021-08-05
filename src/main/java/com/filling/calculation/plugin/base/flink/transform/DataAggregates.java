package com.filling.calculation.plugin.base.flink.transform;


import com.alibaba.fastjson.JSONObject;
import com.filling.calculation.common.CheckConfigUtil;
import com.filling.calculation.common.CheckResult;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.batch.FlinkBatchTransform;
import com.filling.calculation.flink.stream.FlinkStreamTransform;
import com.filling.calculation.flink.util.TableUtil;
import com.filling.calculation.plugin.base.flink.transform.watermark.DefaultWaterMark;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.types.Row;

import java.util.*;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author zihjiang
 */
public class DataAggregates implements FlinkBatchTransform<Row, Row>, FlinkStreamTransform<Row, Row> {

    private JSONObject config;

    private static List<String> SELECT_FIELDS= null;

    private static String ROWTIME_WATERMARK_FIELD;

    private static Long ROWTIME_WATERMARK_FIELD_MS;

    private static Long ROWTIME_WATERMARK_FIELD_DELAY_MS;

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        DataStream<Row> dataStreamForWT = dataStream.assignTimestampsAndWatermarks(new DefaultWaterMark(ROWTIME_WATERMARK_FIELD, ROWTIME_WATERMARK_FIELD_DELAY_MS));
         Table table = tableEnvironment.fromDataStream(
                 dataStreamForWT,
                 _(
                         getGroupField(ROWTIME_WATERMARK_FIELD + ".rowtime", getColumnByTable(tableEnvironment.from(config.getString(SOURCE_TABLE_NAME)), ROWTIME_WATERMARK_FIELD))
                 )
         );
        Table result = table.filter(
                        $(ROWTIME_WATERMARK_FIELD).isNotNull()
        ).window(Tumble.over(lit(ROWTIME_WATERMARK_FIELD_MS).millis()).on($(ROWTIME_WATERMARK_FIELD)).as(ROWTIME_WATERMARK_FIELD + "_watermark")) // define window
                .groupBy( _(getGroupField(ROWTIME_WATERMARK_FIELD + "_watermark", SELECT_FIELDS))) // group by key and window
                .select(
                        _(getSelectField(ROWTIME_WATERMARK_FIELD + "_watermark", SELECT_FIELDS))
                );

        return TableUtil.tableToDataStream(tableEnvironment, result, true);
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();
        // 时间窗口函数不支持batch
        new RuntimeException("时间窗口函数不支持batch");

        return (DataSet<Row>) process(tableEnvironment, data, "batch");
    }

    private Object process(TableEnvironment tableEnvironment, Object data, String type) {

        return "batch".equals(type) ? TableUtil.tableToDataSet((BatchTableEnvironment) tableEnvironment, tableEnvironment.from(config.getString(SOURCE_TABLE_NAME))) : TableUtil.tableToDataStream((StreamTableEnvironment) tableEnvironment, tableEnvironment.from(config.getString(SOURCE_TABLE_NAME)), false);
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
        return CheckConfigUtil.check(config,SOURCE_TABLE_NAME);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        SELECT_FIELDS = config.getObject("group.fields", List.class);

        ROWTIME_WATERMARK_FIELD = config.getString("rowtime.watermark.field");
        ROWTIME_WATERMARK_FIELD_MS = config.getLong("rowtime.watermark.tumble.ms");
        ROWTIME_WATERMARK_FIELD_DELAY_MS = config.getLong("rowtime.watermark.tumble.delay.ms");

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

    /**
     * 返回包括watermark在内的字段(适用于group后)
     * @param watermarkFieldName
     * @param fieldNames
     * @return
     */
    private String getGroupField(String watermarkFieldName,  List<String> fieldNames) {
        List<String> _fieldNames = new ArrayList<>(fieldNames);
        _fieldNames.add(watermarkFieldName);

        System.out.println("getGroupField: " + String.join(",", _fieldNames));

        return String.join(",", _fieldNames);
    }

    /**
     * 返回包括watermark在内的字段(适用于select后)
     * @param watermarkFieldName
     * @param fieldName
     * @return
     */
    private String getSelectField(String watermarkFieldName,  List<String> fieldName) {
        StringBuffer sb = new StringBuffer();


        for (int i = 0; i < SELECT_FIELDS.size(); i++) {
            String field_name = SELECT_FIELDS.get(i);
            List funcs = config.getObject("group." + field_name + ".function", List.class);
            sb.append(field_name);
            sb.append(", ");

            for(int i1=0; i1 < funcs.size(); i1++) {
                String fun = funcs.get(i1).toString();
                sb.append(field_name);
                sb.append(".");
                sb.append(fun);
                sb.append(" as ");
                sb.append(field_name);
                sb.append("_");
                sb.append(fun);
                sb.append(",");
            }
        }

        for (Object obj: config.getObject("custom.field", List.class)) {
            String str = obj.toString();
            sb.append(config.getString("custom.field."+ str +".script"));
            sb.append(" as ");
            sb.append(str);
            sb.append(",");
        }

        sb.append(watermarkFieldName + ".start as " + watermarkFieldName + "_start, ");
        sb.append(watermarkFieldName + ".end as " + watermarkFieldName + "_end");

        System.out.println("getSelectField: " + sb.toString());
        return sb.toString();
    }

    private List<String> getColumnByTable(Table table, String excludeFiledName) {
        List <String> columns = table.getResolvedSchema().getColumnNames();
        // 删除watermark字段
        columns.removeIf(s -> s.contains(excludeFiledName));
        //
//        columns = columns.stream().map(d -> "`"+ d +"`").collect(Collectors.toList());
        return columns;
    }
}


