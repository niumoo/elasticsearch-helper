package com.codingme.eshelper.utils;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.tophits.ParsedTopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.alibaba.fastjson.JSON;
import com.codingme.eshelper.constant.EsConstant;
import com.codingme.eshelper.pojo.EsClientPool;
import com.codingme.eshelper.pojo.EsConstruct;
import com.codingme.eshelper.pojo.EsResult;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * Elasticsearch 查询操作工具类
 *
 * @Author niujinpeng
 * @Date 2019/3/12 16:45
 */
@Slf4j
public class EsUtils {

    private static String HTTP_STATUS_SUCCESS = "OK";

    /**
     * 检查索引是否存在
     *
     * @param esConstruct
     * @return
     * @throws Exception
     */
    public static boolean checkIndexExist(EsConstruct esConstruct) throws Exception {
        Integer clientId = esConstruct.getClientId();
        List<String> indexList = esConstruct.getIndexList();
        RestClient restClient = EsClientPool.getRestClient(clientId);
        boolean exist = true;
        for (String index : indexList) {
            index = java.net.URLEncoder.encode(index, "UTF-8");
            Response response = restClient.performRequest("HEAD", "/" + index);
            exist = exist && StringUtils.equals(response.getStatusLine().getReasonPhrase(), HTTP_STATUS_SUCCESS);
        }
        return exist;
    }

    /**
     * 根据ID删除
     *
     * @param esConstruct
     * @return
     * @throws Exception
     */
    public static EsResult deleteById(EsConstruct esConstruct) throws Exception {
        String transactionId = esConstruct.getTransactionId();
        Integer clientId = esConstruct.getClientId();
        String type = esConstruct.getIndexType();
        String id = esConstruct.getDocumentId();
        List<String> indexList = esConstruct.getIndexList();
        EsResult updateResult = new EsResult();
        if (clientId == null || CollectionUtils.isEmpty(indexList) || StringUtils.isEmpty(type)
            || StringUtils.isEmpty(id)) {
            EsLogUtils.error("[{}]【删除】删除失败,要删除的文档信息不完整,index=[{}],type=[{}],id=[{}]", transactionId, indexList, type,
                id);
            return updateResult;
        }
        String index = indexList.get(0);
        index = java.net.URLEncoder.encode(index, "UTF-8");
        updateResult.setIndexList(Arrays.asList(index));
        updateResult.setClientId(clientId);
        DeleteRequest request = new DeleteRequest(index, type, id);
        DeleteResponse deleteResponse = EsClientPool.getRestHighClient(clientId).delete(request);
        // 删除结果处理
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            EsLogUtils.error("[{}]【删除】删除失败,文档未找到,index=[{}],type=[{}],id=[{}]", transactionId, index, type, id);
            return updateResult;
        }
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            EsLogUtils.error("[{}]【删除】删除失败,分片总量:[{}],成功删除分片总量:[{}]", transactionId, shardInfo.getTotal(),
                shardInfo.getSuccessful());
            return updateResult;
        }
        if (shardInfo.getFailed() > 0) {
            EsLogUtils.error("[{}]【删除】删除失败,要删除的文档,index=[{}],type=[{}],id=[{}]", transactionId, index, type, id);
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
                EsLogUtils.error("[{}]【删除】删除失败,失败原因:[{}]", transactionId, reason);
            }
        } else {
            updateResult.setUpdateCount(1);
            updateResult.setSuccess(true);
        }
        return updateResult;
    }

    /**
     * 根据索引名称删除
     *
     * @param esConstruct
     * @return
     * @throws Exception
     */
    public static EsResult deleteByIndex(EsConstruct esConstruct) throws Exception {
        String transactionId = esConstruct.getTransactionId();
        Integer clientId = esConstruct.getClientId();
        List<String> indexList = esConstruct.getIndexList();
        EsResult updateResult = new EsResult();
        if (CollectionUtils.isEmpty(indexList) || clientId == null) {
            return updateResult;
        }
        updateResult.setClientId(clientId);
        updateResult.setIndexList(indexList);
        long updateCount = 0;
        RestClient restClient = EsClientPool.getRestClient(clientId);
        EsConstruct tempEsConstruct = new EsConstruct();
        tempEsConstruct.setClientId(clientId);
        for (String index : indexList) {
            if (StringUtils.contains(index, ":")) {
                index = java.net.URLEncoder.encode(index, "UTF-8");
            }
            if (StringUtils.contains(index, "*")) {
                EsLogUtils.error("[{}]【删除索引】索引[{}]不能删除", transactionId, index);
                continue;
            }
            try {
                tempEsConstruct.setIndexList(Arrays.asList(index));
                boolean exist = checkIndexExist(tempEsConstruct);
                if (!exist) {
                    EsLogUtils.error("[{}]【删除索引】索引 [{}] 不存在", transactionId, index);
                    continue;
                }
                Response delete = restClient.performRequest("DELETE", "/" + index);
                if (StringUtils.equals(delete.getStatusLine().getReasonPhrase(), HTTP_STATUS_SUCCESS)) {
                    updateCount++;
                }
            } catch (Exception e) {
                EsLogUtils.error("[{}]【删除索引】删除索引[{}]失败", transactionId, JSON.toJSONString(e));
                throw e;
            }
        }
        updateResult.setUpdateCount(updateCount);
        updateResult.setSuccess(true);
        return updateResult;
    }

    /**
     * 根据query语句删除
     *
     * @param esConstruct
     * @return
     */
    public static EsResult deleteByQuery(EsConstruct esConstruct) throws Exception {
        String transactionId = esConstruct.getTransactionId();
        EsResult updateResult = new EsResult();
        Integer clientId = esConstruct.getClientId();
        List<String> indexList = esConstruct.getIndexList();
        String type = esConstruct.getIndexType();
        if (clientId == null || CollectionUtils.isEmpty(indexList) || StringUtils.isEmpty(type)) {
            EsLogUtils.error("[{}]【删除数据】缺少必要参数,clientId=[{}],indexList=[{}],type=[{}]", transactionId, clientId,
                indexList, type);
            return updateResult;
        }
        updateResult.setIndexList(indexList);
        updateResult.setClientId(clientId);
        StringBuilder indexs = new StringBuilder();
        for (String index : indexList) {
            if (StringUtils.contains(index, ":")) {
                index = java.net.URLEncoder.encode(index, "UTF-8");
            }
            if (StringUtils.isEmpty(index) || index.contains("*")) {
                EsLogUtils.error("[{}]【删除数据】要删除的索引不合法,index=[{}]", transactionId, index);
                return updateResult;
            }
            indexs.append("," + index);
        }
        // 匹配的数据总量
        EsResult searchResult = getCount(esConstruct);
        updateResult.setUpdateCount(searchResult.getTotalHits());
        String indexStr = indexs.toString().substring(1);
        String endPoint = "/" + indexStr + "/" + type + "/_delete_by_query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder = EsConstructUtils.handleBoolQuery(esConstruct, searchSourceBuilder);
        HttpEntity entity = new NStringEntity(searchSourceBuilder.toString(), ContentType.APPLICATION_JSON);
        try {
            RestClient restClient = EsClientPool.getRestClient(clientId);
            Response response =
                restClient.performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
            if (StringUtils.equals(response.getStatusLine().getReasonPhrase(), HTTP_STATUS_SUCCESS)) {
                updateResult.setSuccess(true);
            } else {
                updateResult.setSuccess(false);
            }
        } catch (Exception e) {
            EsLogUtils.error("[{}]【删除数据】出现未知异常 [{}]", transactionId, JSON.toJSONString(e));
            throw e;
        }
        return updateResult;
    }

    /**
     * 查询聚合
     *
     * @param esConstruct
     * @return
     */
    public static EsResult getAggregations(EsConstruct esConstruct) throws Exception {
        SearchRequest searchRequest = EsConstructUtils.createSearchRequest(esConstruct);
        // 执行查询
        SearchResponse searchResponse = EsClientPool.getRestHighClient(esConstruct.getClientId()).search(searchRequest);
        EsResult searchResult = EsConstructUtils.createSearchResult(searchResponse, esConstruct, searchRequest);
        searchResult = EsConstructUtils.handlePageInfo(searchResult, esConstruct);
        // 数学聚合结果
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        if (esConstruct.isStatsAggs()) {
            ParsedStats parsedStats = searchResponse.getAggregations().get(EsConstant.AGGREGATIONS_OUTER);
            if (parsedStats != null) {
                searchResult.setStatsResult(parsedStats);
            }
            return searchResult;
        }
        // 去重聚合结果
        if (esConstruct.isCardinalityAggs()) {
            ParsedCardinality parsedCardinality = searchResponse.getAggregations().get(EsConstant.AGGREGATIONS_OUTER);
            if (parsedCardinality != null) {
                searchResult.setTotalHits(parsedCardinality.getValue());
            }
            return searchResult;
        }
        // 普通聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return searchResult;
        }
        Terms byCompanyAggregation = aggregations.get(EsConstant.AGGREGATIONS_OUTER);
        for (Terms.Bucket outBucket : byCompanyAggregation.getBuckets()) {
            Map<String, Object> outerMap = new LinkedHashMap<>();
            outerMap.put(outBucket.getKeyAsString(), outBucket.getDocCount());
            // 聚合统计数据
            if (esConstruct.getTopHitsAggregationBuilder() != null) {
                Aggregations topHitsAggs = outBucket.getAggregations();
                ParsedTopHits parsedTopHits = topHitsAggs.get(EsConstant.AGGREGATIONS_TOP_HITS);
                if (parsedTopHits != null && parsedTopHits.getHits() != null
                    && parsedTopHits.getHits().getHits() != null) {
                    SearchHits searchHits = parsedTopHits.getHits();
                    SearchHit[] hitsHits = searchHits.getHits();
                    for (SearchHit hitsHit : hitsHits) {
                        outerMap = EsConstructUtils.handleSearchHitInfo(outerMap, hitsHit);
                        Map<String, Object> sourceAsMap = hitsHit.getSourceAsMap();
                        if (MapUtils.isNotEmpty(sourceAsMap)) {
                            outerMap.putAll(sourceAsMap);
                        }
                    }
                    outerMap.put(EsConstant.HITS_AGGS_COUNT, outBucket.getDocCount());
                }
            }
            resultList.add(outerMap);
        }
        searchResult.setResultList(resultList);
        return searchResult;
    }

    /**
     * 查询总数
     *
     * @param esConstruct
     * @return
     * @throws Exception
     */
    public static EsResult getCount(EsConstruct esConstruct) throws Exception {
        esConstruct.setEsPage(1);
        esConstruct.setPageSize(0);
        SearchRequest searchRequest = EsConstructUtils.createSearchRequest(esConstruct);
        // 执行查询
        SearchResponse searchResponse = EsClientPool.getRestHighClient(esConstruct.getClientId()).search(searchRequest);
        EsResult searchResult = EsConstructUtils.createSearchResult(searchResponse, esConstruct, searchRequest);
        return searchResult;
    }

    /**
     * 查询列表
     *
     * @param esConstruct
     * @return
     * @throws Exception
     */
    public static EsResult getList(EsConstruct esConstruct) throws Exception {
        SearchRequest searchRequest = EsConstructUtils.createSearchRequest(esConstruct);
        SearchResponse searchResponse = EsClientPool.getRestHighClient(esConstruct.getClientId()).search(searchRequest);
        EsResult searchResult = EsConstructUtils.createSearchResult(searchResponse, esConstruct, searchRequest);
        searchResult = EsConstructUtils.handlePageInfo(searchResult, esConstruct);
        return searchResult;
    }

    /**
     * 查询游标
     *
     * @param esConstruct
     * @return
     */
    public static EsResult getScroll(EsConstruct esConstruct) throws Exception {
        String transactionId = esConstruct.getTransactionId();
        int scrollSize = esConstruct.getScrollSize();
        esConstruct.setPageSize(EsConstant.ES_SCROLL_DEFAULT_SIZE);
        SearchRequest searchRequest = EsConstructUtils.createSearchRequest(esConstruct);
        // 游标使用 _doc 排序会提高查询速度
        searchRequest.source().sort("_doc");
        searchRequest.scroll(TimeValue.timeValueSeconds(EsConstant.SCROLL_KEEP_ALIVE_TIME));
        // 执行查询
        long toolTime = 0L;
        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        SearchResponse searchResponse = EsClientPool.getRestHighClient(esConstruct.getClientId()).search(searchRequest);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        while (searchHits != null && searchHits.length > 0) {
            // 记录查询耗时
            long endTime = System.currentTimeMillis();
            EsLogUtils.info("[{}]【查询游标】查询过程,size=[{}],took=[{}],responseTime=[{}]", transactionId, resultList.size(),
                searchResponse.getTookInMillis(), (endTime - startTime));
            toolTime = toolTime + searchResponse.getTookInMillis();
            resultList.addAll(EsConstructUtils.createResultList(searchResponse));
            if (scrollSize != 0 && resultList.size() > scrollSize) {
                resultList = resultList.subList(0, scrollSize);
                break;
            }
            // 继续拉取游标
            String scrollId = searchResponse.getScrollId();
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(EsConstant.SCROLL_KEEP_ALIVE_TIME));
            startTime = System.currentTimeMillis();
            searchResponse = EsClientPool.getRestHighClient(esConstruct.getClientId()).searchScroll(scrollRequest);
            searchHits = searchResponse.getHits().getHits();
        }
        // 清理游标
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(searchResponse.getScrollId());
        EsClientPool.getRestHighClient(esConstruct.getClientId()).clearScroll(clearScrollRequest);
        EsLogUtils.info("[{}]【查询游标】游标清理完成", transactionId);
        // 返回结果
        EsResult searchResult = EsConstructUtils.createSearchResult(searchResponse, esConstruct, searchRequest);
        // 查询耗时
        searchResult.setTookTime(toolTime);
        // 返回数据
        searchResult.setResultList(resultList);
        EsLogUtils.info("[" + transactionId + "]【查询游标】游标查询结果,[{}]", transactionId, searchResult.printInfo());
        return searchResult;
    }

    /**
     * 批量更新/删除/新增操作
     *
     * @param esConstruct
     * @return
     * @throws Exception
     */
    public static boolean update(EsConstruct esConstruct) throws Exception {
        String transactionId = esConstruct.getTransactionId();
        List<WriteRequest> docList = esConstruct.getDocList();
        Integer clientId = esConstruct.getClientId();
        if (CollectionUtils.isEmpty(docList) || clientId == null) {
            EsLogUtils.error("[{}]【更新】更新列表为空 docList.size=0", transactionId);
            return true;
        }
        BulkRequest request = new BulkRequest();
        // 强制刷新
        if (esConstruct.isRefresh()) {
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        // 拼装数据
        int insertCount = 0;
        int updateCount = 0;
        int deleteCount = 0;
        for (Object object : docList) {
            if (object instanceof IndexRequest) {
                insertCount++;
            } else if (object instanceof UpdateRequest) {
                updateCount++;
            } else if (object instanceof DeleteRequest) {
                deleteCount++;
            }
            DocWriteRequest<?> docWriteRequest = (DocWriteRequest<?>)object;
            request.add(docWriteRequest);
        }
        // 提交数据
        BulkResponse response = EsClientPool.getRestHighClient(clientId).bulk(request);
        List<BulkItemResponse.Failure> failureList = new ArrayList<>();
        // 错误解析
        for (BulkItemResponse bulkItemResponse : response) {
            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                failureList.add(failure);
            }
        }
        String failureJson = JSON.toJSONString(failureList);
        long tookInMillis = response.getTookInMillis();
        boolean hasFailures = response.hasFailures();
        EsLogUtils.info(
            "[{}]【更新】操作完成,insertCount=[{}],updateCount=[{}],deleteCount=[{}],TookInMillis=[{}ms],Refresh=[{}],searchEngineType=[{}]",
            transactionId, insertCount, updateCount, deleteCount, tookInMillis, esConstruct.isRefresh(), clientId);
        if (hasFailures) {
            EsLogUtils.error("[{}]【更新】发现异常,hasFailures=[{}],failureJson=[{}]", transactionId, hasFailures,
                failureJson);
        }
        return !response.hasFailures();
    }

    /**
     * 异步更新方法
     *
     * @param esConstruct
     * @param listener
     * @return
     * @throws Exception
     */
    public static BulkProcessor.Builder updateByAsync(EsConstruct esConstruct, BulkProcessor.Listener listener)
        throws Exception {
        Integer clientId = esConstruct.getClientId();
        ThreadPool threadPool = new ThreadPool(Settings.builder().build());
        return new BulkProcessor.Builder(EsClientPool.getRestHighClient(clientId)::bulkAsync, listener, threadPool);
    }

}