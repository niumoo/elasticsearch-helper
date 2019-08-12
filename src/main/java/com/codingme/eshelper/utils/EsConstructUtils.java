package com.codingme.eshelper.utils;

import com.codingme.eshelper.constant.EsConstant;
import com.codingme.eshelper.pojo.EsConstruct;
import com.codingme.eshelper.pojo.EsResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *
 * @Author niujinpeng
 * @Date 2019/5/30 20:08
 */
public class EsConstructUtils {

    /**
     * 构建返回结果
     *
     * @param searchResponse
     * @return
     */
    public static List<Map<String, Object>> createResultList(SearchResponse searchResponse) throws Exception {
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            if (sourceAsMap == null) {
                sourceAsMap = new HashMap<String, Object>(16);
            }
            sourceAsMap = handleSearchHitInfo(sourceAsMap, hit);
            resultList.add(sourceAsMap);
        }
        return resultList;
    }

    /**
     * 构建查询条件
     *
     * @param esConstruct
     * @return
     */
    public static SearchRequest createSearchRequest(EsConstruct esConstruct) throws Exception {
        long startTime = System.currentTimeMillis();
        // 分页信息处理
        int page = esConstruct.getPage();
        int pageSize = esConstruct.getPageSize();
        int start = (page - 1) < 1 ? 1 : (page - 1) * pageSize;
        esConstruct.setEsPage(start);
        // 创建请求
        SearchRequest searchRequest = new SearchRequest();
        // 查询索引
        String[] indexArr = new String[esConstruct.getIndexList().size()];
        esConstruct.getIndexList().toArray(indexArr);
        searchRequest.indices(indexArr);
        // 文档类型
        searchRequest.types(esConstruct.getIndexType());
        // 构造条件
        SearchSourceBuilder searchSourceBuilder = EsConstructUtils.createSearchSourceBuilder(esConstruct);
        searchRequest.source(searchSourceBuilder);
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
        String transactionId = esConstruct.getTransactionId();
        long endTime = System.currentTimeMillis();
        EsCharUtils.info("[" + transactionId + "]【查询语句】query=[{}],usedTime=[{}]",
            EsCharUtils.compressJson(searchRequest.source().toString()), endTime - startTime);
        return searchRequest;
    }

    /**
     * 构建返回视图结果
     *
     * @param searchResponse
     * @param esConstruct
     * @return
     */
    public static EsResult createSearchResult(SearchResponse searchResponse, EsConstruct esConstruct,
        SearchRequest searchRequest) throws Exception {
        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> resultList = createResultList(searchResponse);
        // 返回结果
        EsResult searchResult = new EsResult();
        // 客户端ID
        searchResult.setClientId(esConstruct.getClientId());
        // 被查索引
        searchResult.setIndexList(esConstruct.getIndexList());
        // 被查类型
        searchResult.setIndexType(esConstruct.getIndexType());
        // 查询耗时
        searchResult.setTookTime(searchResponse.getTookInMillis());
        // 匹配总数
        searchResult.setTotalHits(searchResponse.getHits().getTotalHits());
        // 分页大小
        searchResult.setPageSize(esConstruct.getPageSize());
        // 当前页数
        searchResult.setPage(esConstruct.getPage());
        // 返回数据
        searchResult.setResultList(resultList);
        // 返回query
        searchResult.setSearchQuery(EsCharUtils.compressJson(searchRequest.source().toString()));
        String transactionId = esConstruct.getTransactionId();
        long endTime = System.currentTimeMillis();
        EsCharUtils.info("[" + transactionId + "]【查询结果】[{}],usedTime=[{}]", searchResult.printInfo(),
            endTime - startTime);
        return searchResult;
    }

    /**
     * 构建查询语句 SearchSourceBuilder
     *
     * @return
     */
    public static SearchSourceBuilder createSearchSourceBuilder(EsConstruct esConstruct) {
        // 构造条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.timeout(TimeValue.timeValueMillis(EsConstant.SEARCH_TIME_OUT));
        Integer esPage = esConstruct.getEsPage();
        searchSourceBuilder.from(esPage > 1 ? esPage : esPage - 1).size(esConstruct.getPageSize());
        handleBoolQuery(esConstruct, searchSourceBuilder);
        handleAggs(esConstruct, searchSourceBuilder);
        // 返回字段
        if (CollectionUtils.isNotEmpty(esConstruct.getSourceField())) {
            searchSourceBuilder.storedFields(esConstruct.getSourceField());
        }
        // 排序
        if (esConstruct.getSortBuilder() != null) {
            searchSourceBuilder.sort(esConstruct.getSortBuilder());
        }
        return searchSourceBuilder;
    }

    /**
     * 处理聚合信息
     *
     * @param searchSourceBuilder
     * @return
     */
    public static SearchSourceBuilder handleAggs(EsConstruct esConstruct, SearchSourceBuilder searchSourceBuilder) {
        // 聚合字段
        boolean statsAggs = esConstruct.isStatsAggs();
        boolean cardinalityAggs = esConstruct.isCardinalityAggs();
        TopHitsAggregationBuilder topHitsAggregationBuilder = esConstruct.getTopHitsAggregationBuilder();
        TermsAggregationBuilder aggregationBuilderOuter = esConstruct.getAggregationBuilderOuter();
        if (topHitsAggregationBuilder == null && aggregationBuilderOuter == null) {
            return searchSourceBuilder;
        }
        if (aggregationBuilderOuter != null && !statsAggs && !cardinalityAggs) {
            aggregationBuilderOuter.executionHint("map");
            aggregationBuilderOuter.collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST);
            if (topHitsAggregationBuilder != null) {
                aggregationBuilderOuter.subAggregation(topHitsAggregationBuilder);
            }
            searchSourceBuilder.aggregation(aggregationBuilderOuter);
        } else if (aggregationBuilderOuter != null && statsAggs && !cardinalityAggs) {
            // 统计聚合
            String name = aggregationBuilderOuter.getName();
            String field = aggregationBuilderOuter.field();
            StatsAggregationBuilder statsAgg = AggregationBuilders.stats(name).field(field);
            searchSourceBuilder.aggregation(statsAgg);
        } else if (aggregationBuilderOuter != null && !statsAggs && cardinalityAggs) {
            // 去重聚合
            String name = aggregationBuilderOuter.getName();
            String field = aggregationBuilderOuter.field();
            CardinalityAggregationBuilder cardAgg = AggregationBuilders.cardinality(name).field(field);
            searchSourceBuilder.aggregation(cardAgg);
        }
        esConstruct.setEsPage(1);
        searchSourceBuilder.from(esConstruct.getEsPage() - 1);
        return searchSourceBuilder;
    }

    /**
     * 处理 布尔条件
     *
     * @param searchSourceBuilder
     * @return
     */
    public static SearchSourceBuilder handleBoolQuery(EsConstruct esConstruct,
        SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // AND,OR MUST KEYWORD
        esConstruct.getMustQueryStringSet().forEach(boolQuery::must);

        // AND,OR MUST_NOT KEYWORD
        esConstruct.getMustNotQueryStringSet().forEach(boolQuery::mustNot);

        // MUST
        esConstruct.getMustMap().forEach((key, valueList) -> {
            if (valueList.size() == 1) {
                boolQuery.must(new TermsQueryBuilder(key, valueList.get(0)));
            } else {
                BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
                valueList.forEach((value) -> mustQuery.should(new TermQueryBuilder(key, value)));
                boolQuery.must(mustQuery);
            }
        });

        // MUST_NOT
        esConstruct.getMustNotMap().forEach((key, valueList) -> {
            valueList.forEach((value) -> boolQuery.mustNot(new TermQueryBuilder(key, value)));
        });

        // MUST RANGE
        esConstruct.getMustRangeSet().forEach(boolQuery::must);

        // MUST NOT RANGE
        esConstruct.getMustNotRangeSet().forEach(boolQuery::mustNot);

        // 扩展 Query ,会拼接到 MUST
        esConstruct.getQueryBuildersSet().forEach(boolQuery::must);

        // 扩展 Query ,会拼接到 SHOULD
        esConstruct.getShouldBuilderSet().forEach(boolQuery::should);

        searchSourceBuilder.query(boolQuery);
        return searchSourceBuilder;
    }

    /**
     * 处理分页信息
     *
     * @param searchResult
     * @return
     */
    public static EsResult handlePageInfo(EsResult searchResult, EsConstruct esConstruct) throws Exception {
        int pageSize = esConstruct.getPageSize();
        long totalHits = searchResult.getTotalHits();
        if (pageSize > 0 && totalHits > 0) {
            Double pageSizeDouble = new Double(pageSize);
            Double totalHitsDouble = new Double(totalHits);
            long totalPage = (long)Math.ceil(totalHitsDouble / pageSizeDouble);
            searchResult.setTotalPage(totalPage);
        }
        return searchResult;
    }

    /**
     * 处理命中信息
     *
     * @param map
     * @param hit
     * @return
     */
    public static Map<String, Object> handleSearchHitInfo(Map<String, Object> map, SearchHit hit) throws Exception {
        map.put(EsConstant.HITS_INDEX, hit.getIndex());
        map.put(EsConstant.HITS_TYPE, hit.getType());
        map.put(EsConstant.HITS_ID, hit.getId());
        Map<String, SearchHitField> fieldMap = hit.getFields();
        if (MapUtils.isNotEmpty(fieldMap)) {
            for (Map.Entry<String, SearchHitField> entry : fieldMap.entrySet()) {
                map.put(entry.getKey(), entry.getValue().getValue().toString());
            }
        }
        if (!map.containsKey(EsConstant.SERVICE_ID)) {
            map.put(EsConstant.SERVICE_ID, hit.getId());
        }
        return map;
    }

    /**
     * 解析+|()关键词条件
     *
     * @param name
     * @param value
     * @param slop
     * @return
     * @throws Exception
     */
    public static QueryStringQueryBuilder resolveKeyword(String name, String value, Integer slop) throws Exception {
        if (StringUtils.isEmpty(name) || StringUtils.isEmpty(value)) {
            return null;
        }
        return null;
        //// TODO 需要优化
        // BooleanQuery.Builder builder = new EsParseKeywordUtils().parseStylizedKeyword((Analyzer) null, value, name);
        // QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery(builder.build().toString());
        // if (slop != null) {
        // queryStringQuery.phraseSlop(slop);
        // }
        // return queryStringQuery;
    }

}