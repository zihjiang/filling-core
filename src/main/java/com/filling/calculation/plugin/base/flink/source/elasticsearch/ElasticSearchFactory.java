package com.filling.calculation.plugin.base.flink.source.elasticsearch;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author cclient
 */
public class ElasticSearchInput extends RichParallelSourceFunction<Row> {
    private static RestHighLevelClient client;
    private static ElasticsearchConf elasticsearchConf;
    private static String scrollId;

    public ElasticSearchInput(ElasticsearchConf elasticsearchConf) {
        ElasticSearchInput.elasticsearchConf = elasticsearchConf;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {

        Integer sliceId = getRuntimeContext().getIndexOfThisSubtask();
        Integer sliceMax = getRuntimeContext().getNumberOfParallelSubtasks();
        try {
            client = ElasticsearchUtil.createClient(elasticsearchConf);
            SearchResponse searchResponse;
            // 当并行度大于1时, 使用slice scroll API
            if (sliceMax > 1) {
                searchResponse = ElasticsearchUtil.searchByScroll(client, elasticsearchConf.getIndex(), sliceId, sliceMax);
            } else {
                searchResponse = ElasticsearchUtil.searchByScroll(client, elasticsearchConf.getIndex());
            }
            scrollId = searchResponse.getScrollId();
            if (searchResponse != null) {
                // 第一次查询,
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    Row row = Row.withNames();
                    for (String key : hit.getSourceAsMap().keySet()) {
                        row.setField(key, hit.getSourceAsMap().get(key));
                    }
                    sourceContext.collect(row);
                }
                // 根据ScrollId, 循环查询
                while (true) {
                    List<Map<String, Object>> esData = ElasticsearchUtil.searchByScrollId(client, scrollId);
                    if (esData.size() == 0) {
                        return;
                    }
                    for (Map<String, Object> map : esData) {
                        Row row = Row.withNames();
                        for (Map.Entry entry : map.entrySet()) {
                            row.setField(entry.getKey().toString(), entry.getValue());
                        }
                        sourceContext.collect(row);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        try {
            ElasticsearchUtil.clearScroll(client, scrollId);
        } catch (IOException e) {
            System.out.println("es 关闭失败");
            e.printStackTrace();
        }
    }
}
