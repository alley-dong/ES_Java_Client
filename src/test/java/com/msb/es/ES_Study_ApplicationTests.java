package com.msb.es;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.google.gson.Gson;
import com.msb.es.entity.HySeriesInfo;
import com.msb.es.mapper.HySeriesInfoMapper;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @description: 嗅探器
 * @author: DongCL
 * @date: 2024/5/22 9:59
 */
@SpringBootTest
class ES_Study_ApplicationTests {
    @Resource
    HySeriesInfoMapper hySeriesInfoMapper;

    @Test
    @SneakyThrows
    public void bulkInit() {
        RestHighLevelClient client = ESClient.getInstance().getHighLevelClient();
        GetIndexRequest request = new GetIndexRequest("msb_car_info");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        if (!exists) {
            CreateIndexRequest createRequest = new CreateIndexRequest("msb_car_info");
            createRequest.settings(Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 0)
            );

            createRequest.mapping(
                    "{\n" +
                            "    \"properties\": {\n" +
                            "      \"brandId\": {\n" +
                            "        \"type\": \"long\"\n" +
                            "      },\n" +
                            "      \"brandLogo\": {\n" +
                            "        \"type\": \"text\",\n" +
                            "        \"fields\": {\n" +
                            "          \"keyword\": {\n" +
                            "            \"type\": \"keyword\",\n" +
                            "            \"ignore_above\": 256\n" +
                            "          }\n" +
                            "        }\n" +
                            "      },\n" +
                            "      \"brandName\": {\n" +
                            "        \"type\": \"text\",\n" +
                            "        \"fields\": {\n" +
                            "          \"keyword\": {\n" +
                            "            \"type\": \"keyword\",\n" +
                            "            \"ignore_above\": 256\n" +
                            "          }\n" +
                            "        }\n" +
                            "      },\n" +
                            "      \"fctId\": {\n" +
                            "        \"type\": \"long\"\n" +
                            "      },\n" +
                            "      \"fctName\": {\n" +
                            "        \"type\": \"text\",\n" +
                            "        \"fields\": {\n" +
                            "          \"keyword\": {\n" +
                            "            \"type\": \"keyword\",\n" +
                            "            \"ignore_above\": 256\n" +
                            "          }\n" +
                            "        }\n" +
                            "      },\n" +
                            "      \"id\": {\n" +
                            "        \"type\": \"long\"\n" +
                            "      },\n" +
                            "      \"maxprice\": {\n" +
                            "        \"type\": \"text\",\n" +
                            "        \"fields\": {\n" +
                            "          \"keyword\": {\n" +
                            "            \"type\": \"keyword\",\n" +
                            "            \"ignore_above\": 256\n" +
                            "          }\n" +
                            "        }\n" +
                            "      },\n" +
                            "      \"minprice\": {\n" +
                            "        \"type\": \"text\",\n" +
                            "        \"fields\": {\n" +
                            "          \"keyword\": {\n" +
                            "            \"type\": \"keyword\",\n" +
                            "            \"ignore_above\": 256\n" +
                            "          }\n" +
                            "        }\n" +
                            "      },\n" +
                            "      \"name\": {\n" +
                            "        \"type\": \"text\",\n" +
                            "        \"analyzer\": \"ik_max_word\",\n" +
                            "        \"fields\": {\n" +
                            "          \"keyword\": {\n" +
                            "            \"type\": \"keyword\",\n" +
                            "            \"ignore_above\": 256\n" +
                            "          }\n" +
                            "        }\n" +
                            "      }\n" +
                            "    }\n" +
                            "  }",XContentType.JSON);
            client.indices().create(createRequest, RequestOptions.DEFAULT);
        }

        List<HySeriesInfo> hySeriesInfos = hySeriesInfoMapper.selectList(new LambdaQueryWrapper<>());
        BulkRequest bulkRequest = new BulkRequest("msb_car_info");
        Gson gson = new Gson();
        for (int i = 0; i < hySeriesInfos.size(); i++) {
            bulkRequest.add(new IndexRequest().id(Integer.toString(i)).source(gson.toJson(hySeriesInfos.get(i)), XContentType.JSON));
        }

        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("数量：" + response.getItems().length);

        ESClient.getInstance().closeClient();
    }
}
