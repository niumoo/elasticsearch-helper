package com.codingme.eshelper.utils;

import com.codingme.eshelper.pojo.SearchResult;
import com.codingme.eshelper.constant.SearchConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

/**
 * <p>
 * Elasticsearch 操作工具类
 *
 * @Author niujinpeng
 * @Date 2019/3/12 16:45
 */
@Slf4j
public class SearchUtils {

    /**
     * 查询列表
     *
     * @param searchConstruct
     * @return
     * @throws IOException
     */
    public static SearchResult getList(SearchConstruct searchConstruct) throws IOException {
        SearchRequest searchRequest = createSearchRequest(searchConstruct);
        // 执行查询
        SearchResponse searchResponse = SearchClientPool.getClient().search(searchRequest);
        SearchResult searchResult = createSearchResult(searchResponse, searchConstruct);
        return searchResult;
    }

    /**
     * 查询总数
     *
     * @param searchConstruct
     * @return
     * @throws IOException
     */
    public static SearchResult getCount(SearchConstruct searchConstruct) throws IOException {
        searchConstruct.setPage(1);
        searchConstruct.setPageSize(0);
        SearchRequest searchRequest = createSearchRequest(searchConstruct);
        // 执行查询
        SearchResponse searchResponse = SearchClientPool.getClient().search(searchRequest);
        SearchResult searchResult = createSearchResult(searchResponse, searchConstruct);
        return searchResult;
    }

    /**
     * 查询聚合
     *
     * @param searchConstruct
     * @return
     */
    public static SearchResult getAggregations(SearchConstruct searchConstruct) throws IOException {
        SearchRequest searchRequest = createSearchRequest(searchConstruct);

        // 执行查询
        SearchResponse searchResponse = SearchClientPool.getClient().search(searchRequest);
        SearchResult searchResult = createSearchResult(searchResponse, searchConstruct);

        // 聚合数据
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        Aggregations aggregations = searchResponse.getAggregations();
        Terms byCompanyAggregation = aggregations.get(SearchConstant.AGGREGATIONS_OUTER);
        for (Terms.Bucket outBucket : byCompanyAggregation.getBuckets()) {
            Map<String, Object> outerMap = new LinkedHashMap<>();
            outerMap.put(outBucket.getKeyAsString(), outBucket.getDocCount());
            // 子聚合
            if (searchConstruct.getAggregationList().size() > 1) {
                Aggregations subAggs = outBucket.getAggregations();
                Terms subAgg = subAggs.get(SearchConstant.AGGREGATIONS_INNER);
                Map<String, Object> innerMap = new LinkedHashMap<>();
                for (Terms.Bucket innerBucket : subAgg.getBuckets()) {
                    innerMap.put(innerBucket.getKeyAsString(), innerBucket.getDocCount());
                }
                outerMap.put(SearchConstant.AGGREGATIONS_INNER, innerMap);
            }
            resultList.add(outerMap);
        }
        searchResult.setResultList(resultList);
        return searchResult;
    }

    /**
     * 查询游标
     *
     * @param searchConstruct
     * @return
     */
    public static SearchResult getScroll(SearchConstruct searchConstruct) throws IOException {
        searchConstruct.setPageSize(5000);
        SearchRequest searchRequest = createSearchRequest(searchConstruct);
        searchRequest.scroll(TimeValue.timeValueSeconds(60));
        // 执行查询
        long toolTime = 0L;
        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        SearchResponse searchResponse = SearchClientPool.getClient().search(searchRequest);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        while (searchHits != null && searchHits.length > 0) {
            // 记录查询耗时
            long endTime = System.currentTimeMillis();
            log.info("【查询游标】游标查询过程,took={},responseTime={}", searchResponse.getTookInMillis(), (endTime - startTime));
            toolTime = toolTime + searchResponse.getTookInMillis();

            resultList.addAll(createResultList(searchResponse));

            // 继续拉取游标
            String scrollId = searchResponse.getScrollId();
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(60));
            startTime = System.currentTimeMillis();
            searchResponse = SearchClientPool.getClient().searchScroll(scrollRequest);
            searchHits = searchResponse.getHits().getHits();
        }

        // 清理游标
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(searchResponse.getScrollId());
        ClearScrollResponse clearScrollResponse = SearchClientPool.getClient().clearScroll(clearScrollRequest);
        boolean succeeded = clearScrollResponse.isSucceeded();
        log.info("【查询游标】游标清理完成");

        // 返回结果
        SearchResult searchResult = createSearchResult(searchResponse, searchConstruct);
        // 查询耗时
        searchResult.setTookTime(toolTime);
        // 返回数据
        searchResult.setResultList(resultList);

        log.info("【查询游标】游标查询结果,{}", searchResult.printInfo());
        return searchResult;
    }

    /**
     * 构建查询条件
     *
     * @param searchConstruct
     * @return
     */
    private static SearchRequest createSearchRequest(SearchConstruct searchConstruct) {
        // 创建请求
        SearchRequest searchRequest = new SearchRequest();
        // 查询索引
        String[] indexArr = new String[searchConstruct.getIndexList().size()];
        searchConstruct.getIndexList().toArray(indexArr);
        searchRequest.indices(indexArr);
        // 文档类型
        searchRequest.types(searchConstruct.getIndexType());
        // 构造条件
        SearchSourceBuilder searchSourceBuilder = searchConstruct.createSearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);

        log.info("【查询语句】query={}", searchRequest.source().toString().replaceAll("\r|\n", StringUtils.EMPTY)
                .replace(StringUtils.SPACE, StringUtils.EMPTY));
        return searchRequest;
    }

    /**
     * 构建返回视图结果
     *
     * @param searchResponse
     * @param searchConstruct
     * @return
     */
    private static SearchResult createSearchResult(SearchResponse searchResponse, SearchConstruct searchConstruct) {
        List<Map<String, Object>> resultList = createResultList(searchResponse);
        // 返回结果
        SearchResult searchResult = new SearchResult();
        // 被查索引
        searchResult.setIndexList(searchConstruct.getIndexList());
        // 被查类型
        searchResult.setIndexType(searchConstruct.getIndexType());
        // 查询耗时
        searchResult.setTookTime(searchResponse.getTookInMillis());
        // 匹配总数
        searchResult.setTotalHits(searchResponse.getHits().getTotalHits());
        // 分页大小
        searchResult.setPageSize(searchConstruct.getPageSize());
        // 返回数据
        searchResult.setResultList(resultList);
        log.info("【查询结果】" + searchResult.printInfo());
        return searchResult;
    }

    /**
     * 构建返回结果
     *
     * @param searchResponse
     * @return
     */
    private static List<Map<String, Object>> createResultList(SearchResponse searchResponse) {
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            if (sourceAsMap == null || sourceAsMap.size() == 0) {
                continue;
            }
            sourceAsMap.put("_index", hit.getIndex());
            sourceAsMap.put("_type", hit.getType());
            sourceAsMap.put("_id", hit.getId());
            resultList.add(sourceAsMap);
        }
        return resultList;
    }
}
