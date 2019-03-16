package com.codingme.eshelper.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * <p>
 * 高级连接客户端连接池
 *
 * @Author niujinpeng
 * @Date 2019/3/12 17:07
 */
@Slf4j
public class SearchClientPool {

    private static RestHighLevelClient client = null;
    private static String HOST_NAME = "127.0.0.1";
    private static Integer PORT = 9200;
    private static String SCHEME = "http";

    public static RestHighLevelClient getClient() {
        // TODO 后续改造成连接池
        if (client == null) {
            HttpHost httpHost = new HttpHost(HOST_NAME, PORT, SCHEME);
            RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
            RestClient restClient = restClientBuilder.build();
            client = new RestHighLevelClient(restClient);
            log.info("【高级连接客户端】初始化完毕,address={},port={}", httpHost.getHostName(), httpHost.getPort());
        }
        return client;
    }
}
