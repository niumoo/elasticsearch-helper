package com.codingme.eshelper.utils;

/**
 * <p>
 *
 * @Author niujinpeng
 * @Date 2019/3/19 18:01
 */
public class StringUtils {


    /**
     * 去除JSON 中的换行空格
     *
     * @param json
     * @return
     */
    public static String compressJson(String json) {
        StringBuilder sb = new StringBuilder();
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(json)) {
            json = json.replaceAll("\r|\n", org.apache.commons.lang3.StringUtils.EMPTY);
            char[] chars = json.toCharArray();
            boolean skip = false;
            for (char aChar : chars) {
                if (aChar == '"') {
                    skip = !skip;
                }
                if (skip || (aChar != ' ' && aChar != ' ' && aChar != '\n')) {
                    sb.append(aChar);
                }
            }
        }
        return sb.toString();
    }
}
