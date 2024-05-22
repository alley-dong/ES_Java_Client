package com.msb.es;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;
import org.elasticsearch.client.sniff.Sniffer;

import java.io.IOException;

/**
 * @description: 封装
 * @author: DongCL
 * @date: 2024/5/22 10:30
 */
public class ESClient {

    private static ESClient EsClient;
    /**
     *  保证所有节点都安装了 IK分词器。不然没安装使用IK，插入数据贼慢。
     */
    private String host = "localhost:9200,localhost:9201,localhost:9202";
    private RestClientBuilder builder;
    private static Sniffer sniffer;
    private static RestHighLevelClient highLevelClient;

    /**
     * 私有构造
     */
    private ESClient(){
    }

    /**
     * 单例 懒加载
     */
    public static ESClient getInstance() {
        if (EsClient == null) {
            synchronized (ESClient.class){
                if (EsClient == null) {
                    EsClient = new ESClient();
                    EsClient.getRestClientBuilder();
                }
            }
        }
        return EsClient;
    }

    /**
     * Rest客户端
     */
    public RestClientBuilder getRestClientBuilder() {
        String[] hosts = host.split(",");
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            String[] host = hosts[i].split(":");
            httpHosts[i] = new HttpHost(host[0], Integer.parseInt(host[1]), "http");
        }
        builder = RestClient.builder(httpHosts);

        // 设置请求头
        Header[] defaultHeaders = {
                new BasicHeader("header", "application/json")
        };
        builder.setDefaultHeaders(defaultHeaders);

        return builder;
    }

    /**
     * @description: 高版本客户端
     * @author: DongCL
     * @date: 2024/5/22 10:30
     */
    public RestHighLevelClient getHighLevelClient() {
        if (highLevelClient == null) {
            synchronized (ESClient.class){
                if (highLevelClient == null) {
                    highLevelClient = new RestHighLevelClient(builder);
                    // 每隔5秒嗅探一次
                    sniffer = Sniffer.builder(highLevelClient.getLowLevelClient())
                            .setSniffIntervalMillis(5000)
                            .build();
                }
            }
        }
        return highLevelClient;
    }


    public void closeClient() throws IOException {
        if (highLevelClient != null) {
            highLevelClient.close();
        }
    }
}
