package com.msb.es;

import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @description: 客户端连接 封装
 * @author: DongCL
 * @date: 2024/5/22 9:53
 */
import java.io.IOException;


public class HighLevelClient {
    private static RestClientBuilder restClientBuilder = ClientsBuilders.getRestClientBuilder();

    // 实例化高级客户端
    private static RestHighLevelClient restHighLevelClient;


    public RestHighLevelClient getRestHighLevelClient() {
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        return restHighLevelClient;
    }


    public void closeRestHighLevelClient() throws IOException {
        if (restHighLevelClient != null) {
            restHighLevelClient.close();
        }
    }
}
