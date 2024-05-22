package com.msb.es.controller;

import com.msb.es.dto.ResultDto;
import com.msb.es.util.ESClientUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.io.IOException;

@RestController
@RequestMapping("/client")
public class ClientController {

    private RestHighLevelClient client;

    @PostConstruct
    public void init() {
        this.client = ESClientUtil.getInstance().getHighLevelClient();
    }

    /**
     * 分页查询
     */
    @RequestMapping("/carInfo")
    public ResultDto carInfo(@RequestParam(value = "kw", required = true) String kw,
                             @RequestParam(value = "from", required = true) Integer from,
                             @RequestParam(value = "size", required = true) Integer size) throws IOException {

        SearchRequest searchRequest = new SearchRequest("msb_car_info");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", kw));
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        ResultDto resultDto = new ResultDto();
        resultDto.setData(searchResponse.getHits().getHits());
        return resultDto;
    }


    /**
     * 模糊查询
     */
    @RequestMapping("/fuzzy")
    public SearchHit[] fuzzy(String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest("msb_car_info");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.fuzzyQuery("name", name)
                        .fuzziness(Fuzziness.AUTO)
        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return response.getHits().getHits();
    }


    /**
     * 批量操作
     */
    @RequestMapping("/bulk")
    public ResultDto bulk() throws IOException {
        BulkRequest request = new BulkRequest("msb_car_info");
        request.add(new DeleteRequest("msb_auto", "13"));
        request.add(new UpdateRequest("msb_auto", "22")
                .doc(XContentType.JSON, "name", "宝马666"));
        request.add(new IndexRequest("msb_car_info").id("4")
                .source(XContentType.JSON, "name", "奥迪双钻"));
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulkResponse);
        return null;
    }


    /**
     * 多条件查找
     */
    @RequestMapping("/multiSearch")
    public ResultDto multiSearch() throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        SearchRequest firstSearchRequest = new SearchRequest("msb_car_info");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "朗动"));
        firstSearchRequest.source(searchSourceBuilder);
        request.add(firstSearchRequest);

        SearchRequest secondSearchRequest = new SearchRequest("msb_car_info");
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "揽胜运动"));
        secondSearchRequest.source(searchSourceBuilder);
        request.add(secondSearchRequest);
        MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
        System.out.println(response);
        return null;
    }

    /**
     * 组合查找
     */
    @RequestMapping("/boolSearch")
    public ResultDto boolSearch() throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();

        SearchRequest firstSearchRequest = new SearchRequest("msb_car_info");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query
                (
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.matchQuery("name", "AMG"))
                                .mustNot(QueryBuilders.matchQuery("name", "A级"))
                );
        firstSearchRequest.source(searchSourceBuilder);
        request.add(firstSearchRequest);

        MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
        System.out.println(response);
        return null;
    }
}
