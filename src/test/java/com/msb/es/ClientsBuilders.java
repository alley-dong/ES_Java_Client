package com.msb.es;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;

/**
 * @description: 客户端链接
 * @author: DongCL
 * @date: 2024/5/22 9:55
 */
public class ClientsBuilders {

    private static final String CLUSTER_HOSTNAME = "localhost:9200,localhost:9201,localhost:9202";

    public static void main(String[] args) {
        RestHighLevelClient restHighLevelClient = new HighLevelClient().getRestHighLevelClient();

        System.out.println("");
    }

    public static RestClientBuilder getRestClientBuilder() {
        String[] hosts = CLUSTER_HOSTNAME.split(",");
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            String[] host = hosts[i].split(":");
            httpHosts[i] = new HttpHost(host[0], Integer.parseInt(host[1]), "http");
        }
        RestClientBuilder builder = RestClient.builder(httpHosts);

        // 设置请求头
        Header[] defaultHeaders = {
                new BasicHeader("header", "application/json")
        };
        builder.setDefaultHeaders(defaultHeaders);

        // 设置每次节点发生故障的时候收到通知的监听器，内部探嗅到故障的时候被调用
        builder.setFailureListener(new RestClient.FailureListener(){
           public void onFailure(Node node){
               System.out.println("探嗅回调 -->  error --->");
               super.onFailure(node);
           }
        });

        // 设置修改默认请求配置的回调， 比如请求超时，认证等
        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(10000));

        // 设置向哪些节点发送请求，向哪些节点不发送请求
        builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        return builder;
    }
}
