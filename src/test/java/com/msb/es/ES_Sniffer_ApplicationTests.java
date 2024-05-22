package com.msb.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import java.io.IOException;

/**
 * @description: 嗅探器
 * @author: DongCL
 * @date: 2024/5/22 9:59
 */
@SpringBootTest
class ES_Sniffer_ApplicationTests {

    @Test
    public void sniffer() throws IOException {
        // sniffer监听器
        SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();

        RestClient client = RestClient.builder(
                new HttpHost("localhost", 9200, "http"))
                // 设置用于监听嗅探失败的监听器
                .setFailureListener(sniffOnFailureListener)
                .build();

        NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(
                client,
                ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
                ElasticsearchNodesSniffer.Scheme.HTTPS
        );

        Sniffer sniffer = Sniffer.builder(client)
                // 每隔5秒嗅探一次
                .setSniffIntervalMillis(5000)
                // 嗅探失败的时候，经过设置的时间之后再次嗅探，直至正常为止。
                .setSniffAfterFailureDelayMillis(3000)
                .setNodesSniffer(nodesSniffer)
                .build();

        // 启用监听
        sniffOnFailureListener.setSniffer(sniffer);

        // 注意先后顺序
        sniffer.close();
        client.close();
    }
}
