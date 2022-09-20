package com.lz.demo.sink;

import com.lz.demo.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取
        DataStreamSource<Event> steam = env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Jerry", "/cart", 2000L),
                new Event("Jerry", "/prod?id=1", 3000L),
                new Event("Spike", "/prod?id=10", 4000L),
                new Event("Tom", "/prod?id=10", 5000L),
                new Event("Tom", "/cart", 6000L),
                new Event("Jerry", "/home", 7000L),
                new Event("Jerry", "/home", 8000L)
        );

        // 创建HttpHost
        List<HttpHost> httpHostList = new ArrayList<>();
        httpHostList.add(new HttpHost("192.168.56.3", 9200));

        // 定义ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> eventElasticsearchSinkFunction = (event, runtimeContext, requestIndexer) -> {
            Map<String, String> map = new HashMap<>();
            map.put(event.user, event.url);

            // 构建一个IndexRequest
            IndexRequest request = Requests.indexRequest()
                    .index("clicks")
                    .source(map);
            requestIndexer.add(request);
        };

        steam.addSink(new ElasticsearchSink.Builder<>(httpHostList, eventElasticsearchSinkFunction).build());

        env.execute();
    }
}
