package com.codingme.eshelper.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class EsCharUtils {

    private static final String HTML_SPACE = "&nbsp;";
    private static final Pattern PATTERN_HTML = Pattern.compile("<[^>]+>", Pattern.CASE_INSENSITIVE);

    /**
     * 清理HTML标签
     *
     * @param con
     * @return
     */
    public static String clearHtml(String con) {
        if (StringUtils.isEmpty(con)) {
            return con;
        }
        con = con.replace(HTML_SPACE, StringUtils.EMPTY);
        Matcher mHtml = PATTERN_HTML.matcher(con);
        con = mHtml.replaceAll("");
        int k = con.indexOf("&nbsp;");
        while (k == 0) {
            con = con.substring(6);
            k = con.indexOf("&nbsp;");
        }
        return con;
    }

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