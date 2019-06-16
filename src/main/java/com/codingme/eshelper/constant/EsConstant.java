package com.codingme.eshelper.constant;

/**
 * <p>
 * 查询常量类
 *
 * @Author niujinpeng
 * @Date 2019/3/13 16:21
 */
public class EsConstant {

    /**
     * 子聚合查询名称
     */
    public static String AGGREGATIONS_INNER = "aggs_inner";

    /**
     * 聚合查询最外层名称
     */
    public static String AGGREGATIONS_OUTER = "aggs_aggs";

    /**
     * 聚合查询最外层名称
     */
    public static String AGGREGATIONS_TOP_HITS = "aggs_top_hits";

    /**
     * HTTP 连接超时时间
     */
    public static int CONN_TIME_OUT = 10 * 1000;

    /**
     * 默认查询页码
     */
    public static int DEFAULT_PAGE = 1;

    /**
     * 默认查询数量
     */
    public static int DEFAULT_PAGE_SIZE = 10;

    /**
     * 游标默认大小
     */
    public static int ES_SCROLL_DEFAULT_SIZE = 5000;

    /**
     * 聚合统计数量
     */
    public static final String HITS_AGGS_COUNT = "_count";

    /**
     * 命中ID
     */
    public static String HITS_ID = "_id";
    /**
     * 命中索引
     */
    public static String HITS_INDEX = "_index";

    /**
     * 命中类型
     */
    public static String HITS_TYPE = "_type";

    /**
     * 重试超时时间
     */
    public static int RETRY_TIME_OUT = 2 * 60 * 1000;

    /**
     * 游标生命时间
     */
    public static long SCROLL_KEEP_ALIVE_TIME = 3 * 60 * 1000;

    /**
     * Es 查询超时时间
     */
    public static int SEARCH_TIME_OUT = 3 * 60 * 1000;

    /**
     * 业务ID
     */
    public static String SERVICE_ID = "id";

    /**
     * HTTP 查询超时时间
     */
    public static int SOCKET_TIME_OUT = 1 * 60 * 1000;

}