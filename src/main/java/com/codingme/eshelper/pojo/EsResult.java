package com.codingme.eshelper.pojo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 * elasticsearch 查询操作结果
 *
 * @Author niujinpeng
 * @Date 2019/3/13 16:21
 */
@Getter
@Setter
@ToString
public class EsResult {

    /** 操作的客户端ID */
    private Integer clientId;

    /** 被查索引 */
    private List<String> indexList;

    /** 文档类型 */
    private String indexType;

    /** 当前页数 */
    private int page;

    /** 分页大小 */
    private int pageSize;

    /** 文档结果列表 */
    private List<Map<String, Object>> resultList;

    private String searchQuery;

    /** 数学统计结果 */
    private ParsedStats statsResult;

    /** 操作结果是否成功 */
    private boolean success;

    /** 请求耗时 ms */
    private long tookTime;

    /** 匹配总数 */
    private long totalHits;

    /** 总体页数 */
    private long totalPage;

    /** 操作数据量 */
    private long updateCount;

    public EsResult() {
        indexList = new ArrayList<>();
        indexType = StringUtils.EMPTY;
        resultList = new ArrayList<>();
    }

    public String printInfo() {
        return "EsResult{" + "clientId=" + clientId + ", indexList=" + indexList + ", indexType='" + indexType + '\''
            + ", page=" + page + ", pageSize=" + pageSize + ", resultList="
            + (CollectionUtils.isEmpty(resultList) ? 0 : resultList.size()) + ", success=" + success + ", tookTime="
            + tookTime + ", totalHits=" + totalHits + ", totalPage=" + totalPage + ", updateCount=" + updateCount + '}';
    }

}