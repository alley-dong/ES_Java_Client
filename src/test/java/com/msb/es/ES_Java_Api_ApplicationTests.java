package com.msb.es;

import com.msb.es.entity.Product;
import com.msb.es.service.ProductService;
import lombok.SneakyThrows;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.net.InetAddress;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SpringBootTest
class ES_Java_Api_ApplicationTests {
    @Resource
    private ProductService service;

    //region crud
    @Test
    @SneakyThrows
    void esCRUD() {

        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
//        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300)); // 通讯端口号
        //导入数据
        create(client);
        //查询
        get(client);
        getAll(client);
        update(client);
        delete(client);

        client.close();
        System.out.println(client);
        //Add transport addresses and do something with the client...
    }

    //region create
    @SneakyThrows
    private void create(TransportClient client) {
        List<Product> list = service.list();
        for (Product item : list) {
            System.out.println(item.getDate().toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            IndexResponse response = client.prepareIndex("product", "_doc", item.getId().toString())
                    .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", item.getName())
                            .field("desc", item.getDesc())
                            .field("price", item.getPrice())
                            .field("date", item.getDate().toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
                            .field("tags", item.getTags().replace("\"", "").split(","))
                            .endObject())
                    .get();
            System.out.println(response.getResult());
        }
    }
    //endregion

    //region get
    /*
     * 功能描述: <br>
     * 〈〉
     * @Param: [client]
     * @Return: void
     * @Author: wulei
     * @Date: 2020/6/16 23:28
     */
    @SneakyThrows
    private void get(TransportClient client) {
        GetResponse response = client.prepareGet("product", "_doc", "1").get();
        String index = response.getIndex();//获取索引名称
        String type = response.getType();//获取索引类型
        String id = response.getId();//获取索引id
        System.out.println("index:" + index);
        System.out.println("type:" + type);
        System.out.println("id:" + id);
        System.out.println(response.getSourceAsString());
    }
    //endregion

    //region getAll
    private void getAll(TransportClient client) {
        SearchResponse response = client.prepareSearch("product")
                .get();
        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String res = hit.getSourceAsString();
            System.out.println("res" + res);
        }
    }
    //endregion

    //region update
    @SneakyThrows
    private void update(TransportClient client) {
        UpdateResponse response = client.prepareUpdate("product", "_doc", "2")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "update name")
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }
    //endregion

    //region delete
    @SneakyThrows
    private void delete(TransportClient client) {
        DeleteResponse response = client.prepareDelete("product", "_doc", "2").get();
        System.out.println(response.getResult());
    }
    //endregion

    //endregion

    //region multiSearch
    /*
     * 功能描述: <br>
     * 〈多条件查找〉
     * @Param: []
     * @Return: void
     * @Author: wulei
     * @Date: 2020/6/17 10:02
     */
    @Test
    @SneakyThrows
    void multiSearch() {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        SearchResponse response = client.prepareSearch("product")
                .setQuery(QueryBuilders.termQuery("name", "xiaomi"))//Query
                .setPostFilter(QueryBuilders.rangeQuery("price").from(0).to(4000))
                .setFrom(1).setSize(2)
                .get();
        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String res = hit.getSourceAsString();
            System.out.println("res" + res);
        }
        client.close();
    }
    //endregion

    //region 聚合查询
    /*
     * 功能描述: <br>
     * 〈多条件查找〉
     * @Param: []
     * @Return: void
     * @Author: wulei
     * @Date: 2020/6/17 10:02
     */
    @Test
    @SneakyThrows
    void aggSearch() {
        //region 1->创建客户端连接
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        //endregion

        //region 2->计算并返回聚合分析response对象
        SearchResponse response = client.prepareSearch("product")
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(AggregationBuilders.dateHistogram("group_by_month")
                        .field("date")
                        .calendarInterval(DateHistogramInterval.MONTH)
                        .minDocCount(1)
                        .subAggregation(AggregationBuilders.terms("by_tag")
                                .field("tags.keyword")
                                .subAggregation(AggregationBuilders.avg("avg_price")
                                        .field("price"))
                        )
                ).execute().actionGet();

        //endregion

        //region 3->输出结果信息
        SearchHit[] hits = response.getHits().getHits();
        Map<String, Aggregation> map = response.getAggregations().asMap();
        Aggregation group_by_month = map.get("group_by_month");
        Histogram dates = (Histogram) group_by_month;
        Iterator<Histogram.Bucket> buckets = (Iterator<Histogram.Bucket>) dates.getBuckets().iterator();
        while (buckets.hasNext()) {
            Histogram.Bucket dateBucket = buckets.next();
            System.out.println("\n月份：" + dateBucket.getKeyAsString() + "\n计数：" + dateBucket.getDocCount());
            Aggregation by_tag = dateBucket.getAggregations().asMap().get("by_tag");
            StringTerms terms = (StringTerms) by_tag;
            Iterator<StringTerms.Bucket> tags = terms.getBuckets().iterator();
            while (tags.hasNext()) {
                StringTerms.Bucket tag = tags.next();
                System.out.println("\t变迁名称：" + tag.getKey() + "\n\t数量：" + tag.getDocCount());
                Aggregation avg_price = tag.getAggregations().get("avg_price");
                Avg avg = (Avg) avg_price;
                System.out.println("\t平均价格：" + avg.getValue());
            }
        }
        //endregion

        client.close();


    }
    //endregion
}
