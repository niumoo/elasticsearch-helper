package com.codingme.eshelper.summary;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * 摘要获取入口
 *
 * @Author niujinpeng
 * @Date 2019/8/21 14:50
 */
@Slf4j
public class SummaryMain {

    private static HashMap<String, String> SYMBOL_HTML_MAP = new HashMap<>();
    static {
        SYMBOL_HTML_MAP.put("START_HTML_DUANLUO_P", "<p>");
        SYMBOL_HTML_MAP.put("END_HTML_DUANLUO_P", "</p>");
        SYMBOL_HTML_MAP.put("START_HTML_DUANLUO_BR", "<br/>");
        SYMBOL_HTML_MAP.put("END_HTML_DUANLUO_BR", "</br>");
    }

    /**
     * 使用 IK 分词器，默认高亮标签获取摘要
     *
     * @param content
     *            内容
     * @param summarytLength
     *            摘要最优长度
     * @param keywords
     *            关键词组
     * @return
     * @throws Exception
     */
    public static String getSummary(String content, Integer summarytLength, List<String> keywords) throws Exception {
        return getSummary(content, summarytLength, keywords, LuceneHighlightUtils.PREFIX_TAG,
            LuceneHighlightUtils.SUFFIX_TAG, true);
    }

    /**
     * 使用 IK 分词器，自定义高亮标签获取摘要
     *
     * @param content
     *            内容
     * @param summarytLength
     *            摘要最优长度
     * @param keywords
     *            关键词组
     * @param prefix
     *            高亮前缀
     * @param suffix
     *            高亮后缀
     * @return
     * @throws Exception
     */
    public static String getSummary(String content, Integer summarytLength, List<String> keywords, String prefix,
        String suffix) throws Exception {
        return getSummary(content, summarytLength, keywords, prefix, suffix, true);
    }

    /**
     * 获取摘要
     *
     * @param content
     *            内容
     * @param summarytLength
     *            摘要最优长度
     * @param keywords
     *            关键词组
     * @param prefix
     *            高亮前缀
     * @param suffix
     *            高亮后缀
     * @return
     * @throws Exception
     */
    private static String getSummary(String content, Integer summarytLength, List<String> keywords, String prefix,
        String suffix, boolean useIkAnylyzer) throws Exception {
        if (StringUtils.isEmpty(content)) {
            return content;
        }
        if (StringUtils.isEmpty(prefix) || StringUtils.isEmpty(suffix)) {
            prefix = null;
            suffix = null;
        }
        // 保留指定HTML
        for (Map.Entry<String, String> entry : SYMBOL_HTML_MAP.entrySet()) {
            content = StringUtils.replace(content, entry.getValue(), entry.getKey());
        }
        content = SummaryUtils.clearHtml(content);
        // 还原指定HTML
        for (Map.Entry<String, String> entry : SYMBOL_HTML_MAP.entrySet()) {
            content = StringUtils.replace(content, entry.getKey(), entry.getValue());
        }
        String contentBak = content;
        if (StringUtils.length(content) >= summarytLength) {
            // Lucene 计算摘要
            if (useIkAnylyzer) {
                content = LuceneHighlightUtils.getHighlighter(content, summarytLength, keywords, prefix, suffix);
            } else {
                content = LuceneHighlightUtils.getHighlighterByCjk(content, summarytLength, keywords, prefix, suffix);
            }
            log.debug("【取摘要】Lucene 取摘要结果：{}", content);
            content = SummaryUtils.findSummary(content, summarytLength, prefix, suffix);
            if (StringUtils.isEmpty(content)) {
                content = StringUtils.substring(SummaryUtils.clearHtml(contentBak), 0, summarytLength);
            }
        } else {
            content = SummaryUtils.clearHtml(content);
            content = LuceneHighlightUtils.setHighHlight(content, keywords, prefix, suffix);
        }
        if (StringUtils.isEmpty(content)) {
            content = StringUtils.substring(SummaryUtils.clearHtml(contentBak), 0, summarytLength);
        }
        if (StringUtils.isEmpty(prefix) || StringUtils.isEmpty(suffix)) {
            content = StringUtils.replace(content, LuceneHighlightUtils.PREFIX_TAG, StringUtils.EMPTY);
            content = StringUtils.replace(content, LuceneHighlightUtils.SUFFIX_TAG, StringUtils.EMPTY);
        }
        if (StringUtils.isNotEmpty(content)) {
            // 替换特殊字符
            content = content.replace("<?", StringUtils.EMPTY);
            // 空白非空白
            HashSet<String> spaceSet = new HashSet<>();
            spaceSet.add(StringUtils.SPACE);
            spaceSet.add(" ");
            spaceSet.add("　");
            spaceSet.add(" ");
            for (String space : spaceSet) {
                content = content.replaceAll(space + "+", StringUtils.SPACE);
            }
        }
        return content;
    }

    /**
     * 使用 CJK 分词器，默认高亮标签获取摘要
     *
     * @param content
     *            内容
     * @param summarytLength
     *            摘要最优长度
     * @param keywords
     *            关键词组
     * @return
     * @throws Exception
     */
    public static String getSummaryByCjk(String content, Integer summarytLength, List<String> keywords)
        throws Exception {
        return getSummary(content, summarytLength, keywords, LuceneHighlightUtils.PREFIX_TAG,
            LuceneHighlightUtils.SUFFIX_TAG, false);
    }

    /**
     * 使用 CJK 分词器，自定义高亮标签获取摘要
     *
     * @param content 内容
     * @param summarytLength
     *            摘要最优长度
     * @param keywords
     *            关键词组
     * @param prefix
     *            高亮前缀
     * @param suffix
     *            高亮后缀
     * @return
     * @throws Exception
     */
    public static String getSummaryByCjk(String content, Integer summarytLength, List<String> keywords, String prefix,
        String suffix) throws Exception {
        return getSummary(content, summarytLength, keywords, prefix, suffix, false);
    }
}
