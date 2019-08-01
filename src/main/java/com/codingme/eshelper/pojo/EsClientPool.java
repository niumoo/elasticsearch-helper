package com.codingme.eshelper.pojo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import com.alibaba.fastjson.JSON;
import com.codingme.eshelper.constant.EsConstant;
import com.codingme.eshelper.utils.EsCharUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * 高级连接客户端连接池
 *
 * @Author niujinpeng
 * @Date 2019/3/12 17:07
 */
@Slf4j
public class EsClientPool {

    /**
     * Rest Client map
     */
    private static Map<Integer, RestClient> restClientMap = new ConcurrentHashMap<>();

    /**
     * 高级链接客户端
     */
    private static Map<Integer, RestHighLevelClient> restHighLevelClientMap = new ConcurrentHashMap<>();

    public static RestClient getRestClient(Integer clientId) throws Exception {
        if (!restClientMap.containsKey(clientId)) {
            EsCharUtils.error("【REST连接客户端】当前连接池中不存在这个连接状态,clientId=[{}],当前的连接:[{}]", clientId,
                JSON.toJSONString(restClientMap.keySet()));
        }
        return restClientMap.get(clientId);
    }

    public static RestHighLevelClient getRestHighClient(Integer clientId) throws Exception {
        if (!contains(clientId)) {
            EsCharUtils.error("【高级连接客户端】当前连接池中不存在这个连接状态,clientId=[{}],当前的连接:[{}]", clientId,
                JSON.toJSONString(restHighLevelClientMap.keySet()));
        }
        return restHighLevelClientMap.get(clientId);
    }

    /**
     * 增加一个连接
     * 
     * @param clientId
     *            连接标识
     * @param host
     *            ES 地址
     * @param port
     *            ES 端口
     * @param maxConnPerRoute
     * @param maxConnTotal
     * @param connTimeOut
     * @param socketTimeOut
     * @return
     * @throws Exception
     */
    public static RestHighLevelClient put(Integer clientId, String host, Integer port, Integer maxConnPerRoute,
        Integer maxConnTotal, Integer connTimeOut, Integer socketTimeOut) throws Exception {
        if (!restHighLevelClientMap.containsKey(clientId)) {
            if (StringUtils.isEmpty(host)) {
                EsCharUtils.error("【连接客户端】初始化出错,host is empty");
                return null;
            }
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            HttpHost httpHost = new HttpHost(host, port, "http");
            RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
            restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> {
                requestConfigBuilder.setConnectTimeout(connTimeOut == null ? EsConstant.CONN_TIME_OUT : connTimeOut)
                    .setSocketTimeout(socketTimeOut == null ? EsConstant.SOCKET_TIME_OUT : socketTimeOut)
                    .setConnectionRequestTimeout(0);
                return requestConfigBuilder;
            });
            RestClient restClient = restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setMaxConnPerRoute(maxConnPerRoute == null ? 16 : maxConnPerRoute);
                httpClientBuilder.setMaxConnTotal(maxConnTotal == null ? 32 : maxConnTotal);
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }).setMaxRetryTimeoutMillis(socketTimeOut == null ? EsConstant.RETRY_TIME_OUT : socketTimeOut).build();
            RestHighLevelClient highLevelClient = new RestHighLevelClient(restClient);
            restHighLevelClientMap.put(clientId, highLevelClient);
            restClientMap.put(clientId, restClient);
            EsCharUtils.info("【连接客户端】初始化完毕,clientId=" + clientId + ",address=[{}],port=[{}]", httpHost.getHostName(),
                httpHost.getPort());
        }
        return restHighLevelClientMap.get(clientId);
    }

    public static boolean contains(Integer clientId) throws Exception {
        return restHighLevelClientMap.containsKey(clientId);
    }

    public static void close() {
        if (MapUtils.isNotEmpty(restClientMap)) {
            for (RestClient restClient : restClientMap.values()) {
                try {
                    restClient.close();
                } catch (Exception e) {
                    EsCharUtils.error("在关闭连接客户端时出错");
                    e.printStackTrace();
                }
            }
            restClientMap.clear();
            restHighLevelClientMap.clear();
            EsCharUtils.info("【连接客户端】清理完毕!");
        }
    }

}