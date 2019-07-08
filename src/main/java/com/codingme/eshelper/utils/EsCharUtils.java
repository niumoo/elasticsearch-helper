package com.codingme.eshelper.utils;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * 日志级别-ERROR
     */
    public static String LEVEL_ERROR = "ERROR";
    public static String LEVEL_INFO = "INFO";

    /**
     * 日志最大长度
     */
    public static Integer MAX_LOG_LENGTH = 5000;

    /**
     * 清理HTML标签
     *
     * @param con
     * @return
     */
    public static String clearHtml(String con) {
        if (con == null) {
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
        printLog(LEVEL_ERROR, message, args);
    }

    /**
     * 输出日志
     *
     * @param message
     * @param args
     */
    public static void info(String message, Object... args) {
        printLog(LEVEL_INFO, message, args);
    }

    public static void printLog(String level, String message, Object... args) {
        if (StringUtils.isEmpty(message)) {
            return;
        }
        if (message.length() > MAX_LOG_LENGTH) {
            message = message.substring(0, MAX_LOG_LENGTH) + "...";
        }
        int argsLength = args == null ? 0 : args.length;
        switch (argsLength) {
            case 0:
                if (StringUtils.equals(level, LEVEL_ERROR)) {
                    log.error(message);
                } else {
                    log.info(message);
                }
                break;
            case 1:
                if (StringUtils.equals(level, LEVEL_ERROR)) {
                    log.error(message, args[0]);
                } else {
                    log.info(message, args[0]);
                }
                break;
            case 2:
                if (StringUtils.equals(level, LEVEL_ERROR)) {
                    log.error(message, args[0], args[1]);
                } else {
                    log.info(message, args[0], args[1]);
                }
                break;
            default:
                char[] chars = message.toCharArray();
                StringBuilder logSb = new StringBuilder();
                int index = 0;
                for (int i = 0; i < chars.length; i++) {
                    char aChar = chars[i];
                    if (aChar == '{' && i + 1 < chars.length && chars[i + 1] == '}' && index < argsLength) {
                        logSb.append(args[index++]);
                        i++;
                    } else {
                        logSb.append(aChar);
                    }
                }
                if (StringUtils.equals(level, LEVEL_ERROR)) {
                    log.error(logSb.toString());
                } else {
                    log.info(logSb.toString());
                }
                break;
        }
    }
}