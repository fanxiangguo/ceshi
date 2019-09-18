package com.leyou;

import com.alibaba.fastjson.JSON;
import com.leyou.pojo.Goods;
import com.leyou.repository.GoodsRepository;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringDataEsTest {
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    //根据goods中的注解来创建索引库
    @Test
    public void testAddIndex() {
        elasticsearchTemplate.createIndex(Goods.class);
    }

    //根据goods 中注解生成mapping映射
    @Test
    public void testAddMapping() {
        elasticsearchTemplate.putMapping(Goods.class);
    }

    //获取接口的实现对象 通过java代码对es操作
    @Autowired
    private GoodsRepository goodsRepository;
    //新增或修改
    //@Test
    //public void testAddDoc() {
    //    Goods goods = new Goods("1", "小米9999手机", "手机", "小米", 1199.0, "q3311");
    //    //直接使用操作es的对象调用保存方法进行 新增 修改就行了
    //    goodsRepository.save(goods);
    //}

    //删除
    //@Test
    //public void testDelete() {
    //    goodsRepository.deleteById("1");
    //}

    //批量新增
    @Test
    public void testAddBulkDoc() {
        List<Goods> list = new ArrayList<>();
        list.add(new Goods("1", "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Goods("2", "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Goods("3", "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Goods("4", "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Goods("5", "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        goodsRepository.saveAll(list);
    }

    //普通查询
    @Test
    public void testSearch() {
        //List<Goods> goodsList = goodsRepository.findByTitle("小米");//通过匹配分词title需要定义接口方法
        //    List<Goods> goodsList = goodsRepository.findByBrand("小米");//通过匹配品牌 进行查询
        //    List<Goods> goodsList = goodsRepository.findPriceBetween(2000.0,5000.0);//匹配价格区间
        List<Goods> goodsList = goodsRepository.findByBrandAndPriceBetween("小米", 2000.0, 5000.0);

        //List<Goods> goodsList =  goodsRepository.findByBrandOrPriceBetween("小米",2000.0,5000.0);

        goodsList.forEach(goods -> {
            System.out.println(goods);
        });

    }

    //聚合查询
    @Test
    public void testQuery() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        nativeSearchQueryBuilder.withQuery(QueryBuilders.matchAllQuery());
        nativeSearchQueryBuilder.withPageable(PageRequest.of(0, 2));//分页
        //聚合条件
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("brandCount").field("brand"));
        AggregatedPage<Goods> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), Goods.class);
        //获取聚合结果
        Terms terms = aggregatedPage.getAggregations().get("brandCount");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket -> {
            System.out.println(bucket.getKeyAsString() + ":" + bucket.getDocCount());
        });
    }

    //自定义高亮查询
    @Test
    public void testHighLightQuery() {
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        nativeSearchQueryBuilder.withQuery(QueryBuilders.termQuery("title", "小米"));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        highlightBuilder.field("title");
        nativeSearchQueryBuilder.withHighlightBuilder(highlightBuilder);
        nativeSearchQueryBuilder.withHighlightFields(new HighlightBuilder.Field("title"));
        //spring 整合的elasticSearch 做高亮命中的时候有些麻烦 需要用到 es模板进行操作(构建AggregatedPage进行迂回操作)
        AggregatedPage<Goods> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), Goods.class, new SearchResultMapperImpl());
        List<Goods> goodsList = aggregatedPage.getContent();
        goodsList.forEach(goods -> {
            System.out.println(goods);
        });

    }

    public class SearchResultMapperImpl implements SearchResultMapper {
        @Override
        public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {
            long total = response.getHits().getTotalHits(); //返回值所需参数 提前抽取
            Aggregations aggregations = response.getAggregations();//返回值所需参数 提前抽取
            String scrollId = response.getScrollId();//返回值所需参数 提前抽取
            float maxScore = response.getHits().getMaxScore();//返回值所需参数 提前抽取

            //处理我们想要的结果
            SearchHit[] hits = response.getHits().getHits();
            List<T> content = new ArrayList<>();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();//获取到了 字符串形式的对象
                T t = JSON.parseObject(sourceAsString, clazz); //这样我们就获取到了字符串转成成的 对象
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();//这就和原生操作一样了
                HighlightField highlightField = highlightFields.get("title");
                Text[] fragments = highlightField.getFragments();
                if (fragments != null && fragments.length > 0) {
                    String title = fragments[0].toString();
                    try {
                        BeanUtils.copyProperty(t, "title", title);//将t对象中的tetle全部 替换成 我们遍历出的命中的title 使其被高亮标签包裹
                    } catch (Exception e) {
                        System.out.println("替换title出错");
                    }
                }
                content.add(t);//将替换后的t放入 结果集中 return回去;
            }
            return new AggregatedPageImpl<T>(content,pageable,total,aggregations,scrollId,maxScore);
        }
    }
}
