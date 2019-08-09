package com.codingme.eshelper.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.*;

import java.util.List;

/**
 * <p>
 * Es 的布尔语句操作
 *
 * @Author niujinpeng
 * @Date 2019/7/9 16:03
 */
public class EsBoolQueryBuilderUtils {

    /**
     * 获取一个BoolQuery
     *
     * @return
     */
    public static BoolQueryBuilder create() {
        return QueryBuilders.boolQuery();
    }

    /**
     * 判断一个布尔 query 是否为空
     *
     * @param queryBuilder
     * @return
     */
    public static boolean isEmpty(BoolQueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            return true;
        }
        return !queryBuilder.hasClauses();
    }

    public static boolean isNotEmpty(BoolQueryBuilder queryBuilder) {
        return !isEmpty(queryBuilder);
    }


    public static BoolQueryBuilder must(BoolQueryBuilder queryBuilder, BoolQueryBuilder subQuery) {
        if (!isEmpty(subQuery) && queryBuilder != null) {
            queryBuilder.must(subQuery);
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder must(BoolQueryBuilder queryBuilder, String name, List<?> list) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && CollectionUtils.isNotEmpty(list)) {
            queryBuilder.must(new TermsQueryBuilder(name, list));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder must(BoolQueryBuilder queryBuilder, String name, String value) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
            queryBuilder.must(new TermQueryBuilder(name, value));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder mustNot(BoolQueryBuilder queryBuilder, BoolQueryBuilder subQuery) {
        if (!isEmpty(subQuery) && queryBuilder != null) {
            queryBuilder.mustNot(subQuery);
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder mustNot(BoolQueryBuilder queryBuilder, String name, List<?> list) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && CollectionUtils.isNotEmpty(list)) {
            queryBuilder.mustNot(new TermsQueryBuilder(name, list));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder mustNot(BoolQueryBuilder queryBuilder, String name, String value) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
            queryBuilder.mustNot(new TermQueryBuilder(name, value));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder should(BoolQueryBuilder queryBuilder, BoolQueryBuilder subQuery) {
        if (!isEmpty(subQuery) && queryBuilder != null) {
            queryBuilder.should(subQuery);
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder should(BoolQueryBuilder queryBuilder, String name, List<?> list) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && CollectionUtils.isNotEmpty(list)) {
            queryBuilder.should(new TermsQueryBuilder(name, list));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder should(BoolQueryBuilder queryBuilder, String name, String value) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
            queryBuilder.should(new TermQueryBuilder(name, value));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder filter(BoolQueryBuilder queryBuilder, BoolQueryBuilder subQuery) {
        if (!isEmpty(subQuery) && queryBuilder != null) {
            queryBuilder.filter(subQuery);
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder filter(BoolQueryBuilder queryBuilder, String name, List<?> list) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && CollectionUtils.isNotEmpty(list)) {
            queryBuilder.filter(new TermsQueryBuilder(name, list));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder filter(BoolQueryBuilder queryBuilder, String name, String value) {
        if (queryBuilder != null && StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
            queryBuilder.filter(new TermQueryBuilder(name, value));
        }
        return queryBuilder;
    }
}
