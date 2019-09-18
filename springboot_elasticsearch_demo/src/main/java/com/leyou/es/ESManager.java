package com.leyou.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leyou.pojo.Item;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ESManager {
    RestHighLevelClient client = null;

    @Before
    public void init() throws Exception {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9201, "http"),
                        new HttpHost("127.0.0.1", 9202, "http"),
                        new HttpHost("127.0.0.1", 9203, "http")));
    }

    //新增
    @Test
    public void testDoc() throws Exception {
        Item item = new Item("1", "小米9手机", "手机", "小米", 1199.0, "22流浪者");
        //IndexRequest 是专门用来插入索引数据的对象
        IndexRequest indexRequest = new IndexRequest("item", "docs", item.getId());
        //把对象转换成json字符串放入索引库
        String jsonString = JSON.toJSONString(item); //这是 fastJson包下的转换方式
        //将转好的数据放入到索引库中
        indexRequest.source(jsonString, XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    //删除
    @Test
    public void testDelete() throws Exception {
        DeleteRequest request = new DeleteRequest("item", "docs", "1");
        //调用客户端进行删除
        client.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    //批量新增
    public void testBulkAddDoc() throws Exception {
        List<Item> list = new ArrayList<Item>();
        list.add(new Item("1", "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item("2", "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item("3", "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item("4", "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item("5", "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        BulkRequest bulkRequest = new BulkRequest();
        list.forEach(item -> {
            IndexRequest request = new IndexRequest("item", "docs", item.getId());
            String jsonString = JSON.toJSONString(item);
            request.source(jsonString, XContentType.JSON);
            bulkRequest.add(request);
        });
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testSearch() throws Exception {
        //构建一个用来查询的对象
        SearchRequest searchRequest = new SearchRequest("item").types("docs");
        //构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构建具体的查询条件
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());//查询所有
        //searchSourceBuilder.query(QueryBuilders.termQuery("title","小米"));//查询分词条件
        //searchSourceBuilder.query(QueryBuilders.wildcardQuery("title","*为*"));//通配符查询
        //FuzzyQueryBuilder fuzziness = new FuzzyQueryBuilder("title", "d米").fuzziness(Fuzziness.ONE);
        //searchSourceBuilder.query(fuzziness);//模糊查询
       /*
        显示字段的过滤
        searchSourceBuilder.query(QueryBuilders.termQuery("title","小米"));
        searchSourceBuilder.fetchSource(new String[]{"id","title"},null);//包含条件的
        searchSourceBuilder.fetchSource(null,new String[]{"id","title"});//排除条件的
        */

        /*
        过滤出包含条件的
        searchSourceBuilder.query(QueryBuilders.termQuery("title","手机"));
        searchSourceBuilder.postFilter(QueryBuilders.termQuery("brand","锤子"));//过滤出来名中条件的
        */
       /*
       分页查询
       searchSourceBuilder.query(QueryBuilders.matchAllQuery());//查询所有
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);//分页
        */
        /*
        排序
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());//查询所有
        searchSourceBuilder.sort("price", SortOrder.DESC);//排序
        */
      /*
      //高亮显示
        searchSourceBuilder.query(QueryBuilders.termQuery("title","小米"));//查询分词条件
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style = 'color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);
        */
      //聚合查询
        searchSourceBuilder.aggregation(AggregationBuilders.terms("brandCound").field("brand"));


        //放入到searchRequest 中
        searchRequest.source(searchSourceBuilder);
        //执行查询 获取搜索相应结果
        SearchResponse searchResponsce = client.search(searchRequest,RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponsce.getAggregations();
        Terms terms = aggregations.get("brandCound");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println(bucket.getKeyAsString() + ":" + bucket.getDocCount());
        });

        long totalHits = searchResponsce.getHits().totalHits;
        System.out.println(totalHits);//获取命中对象的总数
        System.out.println("*****************");
        //获取到搜索结果的hits域 用来生成josn对象
        SearchHit[] hits = searchResponsce.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            //将json字符串转换成为Item对象.
            Item item = JSON.parseObject(sourceAsString,Item.class);
         /*   //获取到hit域中的 高亮域
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //高亮域名中是以Map形式进行保存的 根据Key值获取到具体高亮的内容
            HighlightField highlightField = highlightFields.get("title");
            //获取到的是个数组
            Text[] fragments = highlightField.getFragments();
            if (fragments!=null&&fragments.length>0){//健壮性判断
                String title = fragments[0].toString();//将高亮内容转化成为字符串形式
                item.setTitle(title);//替换对象中的属性 使其高亮
            }*/

         System.out.println(item);
        }
    }

    @After
    public void end() throws Exception {
        client.close();
    }
}


