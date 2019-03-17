package com.codingme.eshelper.pojo;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 查询返回实体类
 *
 * @Author niujinpeng
 * @Date 2019/3/13 16:21
 */
@Getter
@Setter
@ToString
public class SearchResult {

    /** 被查索引 */
    private List indexList = new ArrayList<String>();

    /** 文档类型 */
    private String indexType = StringUtils.EMPTY;

    /** 请求耗时 ms */
    private long tookTime;

    /** 匹配总数 */
    private long totalHits;

    /** 分页大小 */
    private int pageSize;

    /** 返回文档 */
    private List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();

    public String printInfo() {
        return "SearchResult{" + "indexList=" + indexList + ", indexType='" + indexType + '\'' + ", tookTime="
            + tookTime + ", totalHits=" + totalHits + ", pageSize=" + pageSize + '}';
    }
}
