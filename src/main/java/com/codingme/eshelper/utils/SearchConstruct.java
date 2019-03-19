package com.codingme.eshelper.utils;

import com.codingme.eshelper.constant.SearchConstant;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 查询条件构造器
 *
 * @Author niujinpeng
 * @Date 2019/3/12 17:20
 */
@Setter
@Getter
@ToString
public class SearchConstruct {

    /**
     * AND 关系
     */
    private Map<String, List<String>> mustMap = new HashMap<String, List<String>>();

    /**
     * MUST_NOT 关系
     */
    private Map<String, List<String>> mustNotMap = new HashMap<String, List<String>>();

    /**
     * RAGNE 关系
     */
    private Set<RangeQueryBuilder> rangeSet = new HashSet<RangeQueryBuilder>();

    /**
     * 默认排序
     */
    public FieldSortBuilder sortBuilder = new FieldSortBuilder("id").order(SortOrder.ASC);

    /**
     * 被查索引
     */
    private List indexList = new ArrayList<String>();

    /**
     * 文档类型
     */
    private String indexType = StringUtils.EMPTY;

    /**
     * 当前页码
     */
    private Integer page = 1;

    /**
     * 返回数量
     */
    private Integer pageSize = 10;

    /**
     * 返回字段
     */
    private List<String> sourceField = Arrays.asList("_id");

    /**
     * 聚合字段
     */
    private List<String> aggregationList = new ArrayList<>();

    /**
     * 构建查询语句 SearchSourceBuilder
     *
     * @return
     */
    public SearchSourceBuilder createSearchSourceBuilder() {
        // 构造条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(page - 1).size(pageSize);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // MUST
        mustMap.forEach((key, valueList) -> {
            BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
            valueList.forEach((value) -> mustQuery.should(new TermQueryBuilder(key, value)));
            boolQuery.must(mustQuery);
        });
        // MUST_NOT
        mustNotMap.forEach((key, valueList) -> {
            valueList.forEach((value) -> boolQuery.mustNot(new TermQueryBuilder(key, value)));
        });
        // RANGE
        rangeSet.forEach(rangeQueryBuilder -> boolQuery.must(rangeQueryBuilder));

        // 聚合字段
        if (CollectionUtils.isNotEmpty(aggregationList)) {
            TermsAggregationBuilder aggregation =
                    AggregationBuilders.terms(SearchConstant.AGGREGATIONS_OUTER).field(aggregationList.get(0));
            if (aggregationList.size() > 1) {
                aggregation.subAggregation(
                        AggregationBuilders.terms(SearchConstant.AGGREGATIONS_INNER).field(aggregationList.get(1)));
            }
            aggregation.size(pageSize);
            searchSourceBuilder.aggregation(aggregation);
            setPage(1);
            searchSourceBuilder.from(page - 1).size(0);
        }

        // 返回字段
        if (CollectionUtils.isNotEmpty(sourceField)) {
            String[] sourceFieldsArr = sourceField.stream().toArray(String[]::new);
            searchSourceBuilder.fetchSource(sourceFieldsArr, null);
        }
        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        return searchSourceBuilder;
    }

    public SearchConstruct must(String name, List<String> valueList) {
        mustMap.put(name, valueList);
        return this;
    }

    public SearchConstruct must(String name, String value) {
        mustMap.put(name, Arrays.asList(value));
        return this;
    }

    public SearchConstruct mustNot(String name, List<String> valueList) {
        mustNotMap.put(name, valueList);
        return this;
    }

    public SearchConstruct mustNot(String name, String value) {
        mustNotMap.put(name, Arrays.asList(value));
        return this;
    }

    /**
     * 范围查询
     *
     * @param name         字段名称
     * @param lowerTerm    开始值
     * @param upperTerm    结束值
     * @param includeLower 是否包含开始值
     * @param includeUpper 是否包含结束值
     * @return
     */
    public SearchConstruct range(String name, String lowerTerm, String upperTerm, boolean includeLower,
                                 boolean includeUpper) {
        RangeQueryBuilder rangeQueryBuilder =
                QueryBuilders.rangeQuery(name).from(lowerTerm).to(upperTerm).includeLower(true).includeUpper(true);
        rangeSet.add(rangeQueryBuilder);
        return this;
    }
}
