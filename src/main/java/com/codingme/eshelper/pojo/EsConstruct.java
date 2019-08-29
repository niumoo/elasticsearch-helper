package com.codingme.eshelper.pojo;

import com.codingme.eshelper.constant.EsConstant;
import com.codingme.eshelper.utils.EsLogUtils;
import com.codingme.eshelper.utils.EsConstructUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.BooleanClause;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

/**
 * <p>
 * 查询条件构造器
 *
 * @Author niujinpeng
 * @Date 2019/3/12 17:20
 */
@Getter
@Setter
@ToString
@Slf4j
public class EsConstruct {

    /**
     * AGGS 外层聚合
     */
    private TermsAggregationBuilder aggregationBuilderOuter;

    /**
     * 是否去重聚合
     */
    private boolean cardinalityAggs;

    /**
     * 连接客户端 ID
     */
    private Integer clientId;

    /**
     * 批量DOC
     */
    private List<WriteRequest> docList;

    /**
     * 文档 ID
     */
    private String documentId;

    /**
     * ES 查询页码
     */
    private Integer esPage;

    /**
     * 被查索引
     */
    private List<String> indexList;

    /**
     * 文档类型
     */
    private String indexType;

    /**
     * MUST OR FIELD 关系
     */
    private Map<String, List<String>> mustMap;

    /**
     * MUST_NOT OR 关系
     */
    private Map<String, List<String>> mustNotMap;

    /**
     * AND,OR KEYWORD FILTER 字段
     */
    private Set<QueryStringQueryBuilder> mustNotQueryStringSet;

    /**
     * MUST NOT RAGNE 关系
     */
    private Set<RangeQueryBuilder> mustNotRangeSet;

    /**
     * AND,OR KEYWORD MUST 字段
     */
    private Set<QueryStringQueryBuilder> mustQueryStringSet;

    /**
     * MUST RAGNE 关系
     */
    private Set<RangeQueryBuilder> mustRangeSet;

    /**
     * 当前页码
     */
    private Integer page;

    /**
     * 返回数量
     */
    private Integer pageSize;

    /**
     * 扩展 Query ,会拼接到 MUST
     */
    private Set<QueryBuilder> queryBuildersSet;
    /**
     * 是否强制刷新
     */
    private boolean refresh;

    /**
     * 游标默认返回数量 0 为不做限制
     */
    private int scrollSize = 0;

    /**
     * 扩展 Query ,会拼接到 SHOULD
     */
    private Set<QueryBuilder> shouldBuilderSet;

    /**
     * 排序字段
     */
    public FieldSortBuilder sortBuilder;

    /**
     * 返回字段
     */
    private List<String> sourceField;

    /**
     * 是否数学聚合
     */
    private boolean statsAggs;

    /**
     * AGGS 聚合数据
     */
    private TopHitsAggregationBuilder topHitsAggregationBuilder;

    /**
     * 事务Id
     */
    private String transactionId;

    public EsConstruct() {
        this.esPage = EsConstant.DEFAULT_PAGE;
        this.indexList = new ArrayList<String>();
        this.indexType = StringUtils.EMPTY;
        this.mustMap = new HashMap<String, List<String>>();
        this.mustNotMap = new HashMap<String, List<String>>();
        this.mustNotQueryStringSet = new HashSet<QueryStringQueryBuilder>();
        this.mustNotRangeSet = new HashSet<RangeQueryBuilder>();
        this.mustQueryStringSet = new HashSet<QueryStringQueryBuilder>();
        this.mustRangeSet = new HashSet<RangeQueryBuilder>();
        this.page = EsConstant.DEFAULT_PAGE;
        this.pageSize = EsConstant.DEFAULT_PAGE_SIZE;
        this.queryBuildersSet = new HashSet<QueryBuilder>();
        this.shouldBuilderSet = new HashSet<QueryBuilder>();
        this.sourceField = new ArrayList<String>();
    }

    /**
     * 设置聚合字段
     *
     * @param aggsField
     * @param size
     * @param orderField
     * @param desc
     * @return
     */
    public EsConstruct aggs(String aggsField, Integer size, String orderField, boolean desc) {
        if (StringUtils.isEmpty(aggsField)) {
            return this;
        }
        if (size == null) {
            size = 10;
        }
        TermsAggregationBuilder aggs = AggregationBuilders.terms(EsConstant.AGGREGATIONS_OUTER).field(aggsField)
                .size(size);
        this.aggregationBuilderOuter = aggs;
        if (StringUtils.isNotEmpty(orderField)) {
            if (desc) {
                aggs.order(Terms.Order.aggregation(orderField, !desc));
            } else {
                aggs.order(Terms.Order.aggregation(orderField, desc));
            }
        }
        return this;
    }

    public EsConstruct aggsTopHit(Integer size, String orderField, boolean desc) {
        if (size == null) {
            size = 10;
        }
        TopHitsAggregationBuilder topHitsAggs = AggregationBuilders.topHits(EsConstant.AGGREGATIONS_TOP_HITS).from(0)
                .size(size);
        if (StringUtils.isNotEmpty(orderField)) {
            if (desc) {
                topHitsAggs.sort(orderField, SortOrder.DESC);
            } else {
                topHitsAggs.sort(orderField, SortOrder.ASC);
            }
        }
        this.topHitsAggregationBuilder = topHitsAggs;
        return this;
    }

    /**
     * 范围查询
     *
     * @param name         字段名称
     * @param lower        开始值
     * @param upper        结束值
     * @param includeLower 是否包含开始值
     * @param includeUpper 是否包含结束值
     * @return
     */
    private EsConstruct handleRange(String name, Object lower, Object upper, boolean includeLower, boolean includeUpper,
                                    BooleanClause.Occur occur) {
        if (lower == null && upper == null) {
            return this;
        }
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(name).includeLower(includeLower)
                .includeUpper(includeUpper);
        if (lower != null) {
            rangeQueryBuilder.from(lower);
        }
        if (upper != null) {
            rangeQueryBuilder.to(upper);
        }
        if (occur.equals(BooleanClause.Occur.MUST)) {
            this.mustRangeSet.add(rangeQueryBuilder);
        } else {
            this.mustNotRangeSet.add(rangeQueryBuilder);
        }
        return this;
    }

    /**
     * 扩展 QueryBuilder,用于复杂自定义查询
     *
     * @param queryBuilder
     * @return
     */
    public EsConstruct must(QueryBuilder queryBuilder) {
        if (queryBuilder != null) {
            this.queryBuildersSet.add(queryBuilder);
        }
        return this;
    }

    public EsConstruct must(String name, List<String> valueList) {
        if (StringUtils.isNotEmpty(name) && CollectionUtils.isNotEmpty(valueList)) {
            this.mustMap.put(name, valueList);
        }
        return this;
    }

    public EsConstruct must(String name, String value) {
        if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
            this.mustMap.put(name, Arrays.asList(value));
        }
        return this;
    }

    /**
     * AND,OR KEYWORD 关键词查询
     *
     * @param name
     * @param value
     * @return
     */
    public EsConstruct mustKeyword(String name, String value) {
        return mustKeyword(name, value, null);
    }

    public EsConstruct mustKeyword(String name, String value, Integer slop) {
        QueryStringQueryBuilder stringQueryBuilder = null;
        try {
            stringQueryBuilder = EsConstructUtils.resolveKeyword(name, value, slop);
            if (stringQueryBuilder != null) {
                mustQueryStringSet.add(stringQueryBuilder);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    public EsConstruct mustNot(String name, List<String> valueList) {
        if (StringUtils.isNotEmpty(name) && CollectionUtils.isNotEmpty(valueList)) {
            this.mustNotMap.put(name, valueList);
        }
        return this;
    }

    public EsConstruct mustNot(String name, String value) {
        if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
            this.mustNotMap.put(name, Arrays.asList(value));
        }
        return this;
    }

    /***
     * AND,OR 排除关键词
     */
    public EsConstruct mustNotKeyword(String name, String value) {
        return mustNotKeyword(name, value, null);
    }

    public EsConstruct mustNotKeyword(String name, String value, Integer slop) {
        QueryStringQueryBuilder stringQueryBuilder = null;
        try {
            stringQueryBuilder = EsConstructUtils.resolveKeyword(name, value, slop);
            if (stringQueryBuilder != null) {
                mustNotQueryStringSet.add(stringQueryBuilder);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * 范围查询
     *
     * @param name  字段名称
     * @param lower 开始值
     * @param upper 结束值
     * @return
     */
    public EsConstruct mustNotRange(String name, Object lower, Object upper) {
        return mustNotRange(name, lower, upper, true, true);
    }

    /**
     * 范围查询
     *
     * @param field        字段名称
     * @param lower        开始值
     * @param upper        结束值
     * @param includeLower 是否包含开始值
     * @param includeUpper 是否包含结束值
     * @return
     */
    public EsConstruct mustNotRange(String field, Object lower, Object upper, boolean includeLower,
                                    boolean includeUpper) {
        return handleRange(field, lower, upper, includeLower, includeLower, BooleanClause.Occur.MUST_NOT);
    }

    /**
     * 范围查询
     *
     * @param name  字段名称
     * @param lower 开始值
     * @param upper 结束值
     * @return
     */
    public EsConstruct mustRange(String name, Object lower, Object upper) {
        return mustRange(name, lower, upper, true, true);
    }

    /**
     * 范围查询
     *
     * @param name         字段名称
     * @param lower        开始值
     * @param upper        结束值
     * @param includeLower 是否包含开始值
     * @param includeUpper 是否包含结束值
     * @return
     */
    public EsConstruct mustRange(String name, Object lower, Object upper, boolean includeLower, boolean includeUpper) {
        return handleRange(name, lower, upper, includeLower, includeLower, BooleanClause.Occur.MUST);
    }

    /**
     * 扩展 QueryBuilder,用于复杂自定义查询
     *
     * @param queryBuilder
     * @return
     */
    public EsConstruct should(QueryBuilder queryBuilder) {
        if (queryBuilder != null) {
            this.shouldBuilderSet.add(queryBuilder);
        }
        return this;
    }

    /**
     * 自定义排序
     *
     * @param field 排序字段
     * @param desc  是否降序
     */
    public EsConstruct sort(String field, boolean desc) {
        if (StringUtils.isEmpty(field)) {
            EsLogUtils.error("【设置排序】排序字段为空");
            return this;
        }
        this.sortBuilder = new FieldSortBuilder(field);
        if (desc) {
            this.sortBuilder.order(SortOrder.DESC);
        } else {
            this.sortBuilder.order(SortOrder.ASC);
        }
        return this;
    }

}