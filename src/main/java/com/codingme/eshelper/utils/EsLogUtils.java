package com.codingme.eshelper.utils;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * 字符工具类
 *
 * @Author niujinpeng
 * @Date 2019/4/10 14:55
 */
@Slf4j
public class EsLogUtils {

    /**
     * 去除JSON 中的换行以及多余空格
     *
     * @param json
     * @return
     */
    public static String compressJson(String json) {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(json)) {
            json = json.replaceAll("\r|\n", StringUtils.EMPTY);
            char[] chars = json.toCharArray();
            boolean skip = false;
            for (char aChar : chars) {
                if (aChar == '"') {
                    skip = !skip;
                }
                if (skip || (aChar != ' ' && aChar != '\n')) {
                    sb.append(aChar);
                }
            }
        }
        return sb.toString();
    }

    public static void error(String message, Object... args) {
        log.error(message, args);
    }

    public static void info(String message, Object... args) {
        log.info(message, args);
    }
}