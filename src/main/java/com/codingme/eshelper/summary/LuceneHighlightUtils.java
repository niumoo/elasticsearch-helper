package com.codingme.eshelper.summary;

import java.io.StringReader;
import java.util.List;

import com.codingme.eshelper.ik.IkAnalyzer6x;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.cfg.DefaultConfig;
import org.wltea.analyzer.dic.Dictionary;

/**
 * <p>
 * 使用 Lucene 进行内容高亮并取指定长度的摘要信息
 *
 * @Author niujinpeng
 * @Date 2019/8/21 10:20
 */
public class LuceneHighlightUtils {

    /**
     * CJK 分词器
     */
    private static Analyzer cjkAnalyzer = new CJKAnalyzer();
    /**
     * IK 分词器
     */
    private static Analyzer ikAnalyzer = new IkAnalyzer6x(false);

    /**
     * IK 智能分词模式
     */
    private static Analyzer smartIkAnalyzer = new IkAnalyzer6x(true);
    /**
     * 默认长度
     */
    public static int CONTENT_LENGTH = 200 * 2;
    /**
     * 高亮后缀
     */
    public static String SUFFIX_TAG = "</font>";
    /**
     * 高亮前缀
     */
    public static String PREFIX_TAG = "<font color=\"red\">";

    static {
        Configuration cfg = DefaultConfig.getInstance();
        Dictionary.initial(cfg);
    }

    private static String getHighlighter(String content, Integer contentLength, List<String> keywords,
        boolean useIkAnalyzer, boolean smart) throws Exception {
        BooleanQuery.Builder booleanQueryBuild = new BooleanQuery.Builder();
        Analyzer analyzer = useIkAnalyzer ? (smart ? smartIkAnalyzer : ikAnalyzer) : cjkAnalyzer;
        if (keywords != null && keywords.size() > 0) {
            Dictionary.getSingleton().addWords(keywords);
            for (String tmp : keywords) {
                QueryParser queryParser = new QueryParser(StringUtils.EMPTY, analyzer);
                queryParser.setPhraseSlop(0);
                queryParser.setDefaultOperator(QueryParser.Operator.AND);
                Query query = queryParser.parse(tmp);
                BooleanClause booleanClause = new BooleanClause(query, Occur.SHOULD);
                booleanQueryBuild.add(booleanClause);
            }
        }
        contentLength = contentLength == null ? CONTENT_LENGTH : contentLength;

        BooleanQuery build = booleanQueryBuild.build();
        QueryScorer scorer = new QueryScorer(build);
        SimpleSpanFragmenter simpleFragmenter = new SimpleSpanFragmenter(scorer, contentLength);
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(StringUtils.EMPTY, StringUtils.EMPTY);
        Highlighter highlighter = new Highlighter(simpleHTMLFormatter, scorer);
        highlighter.setTextFragmenter(simpleFragmenter);
        TokenStream tokenStream = analyzer.tokenStream(StringUtils.EMPTY, new StringReader(content));
        return highlighter.getBestFragment(tokenStream, content);
    }

    /**
     * 使用 ik 分词器计算摘要信息
     *
     * @param content
     * @param contentLength
     * @param keywords
     * @return
     * @throws Exception
     */
    public static String getHighlighter(String content, Integer contentLength, List<String> keywords) throws Exception {
        return getHighlighter(content, contentLength, keywords, null, null);
    }

    /**
     * 使用 ik 分词器计算摘要信息
     *
     * @param content
     * @param contentLength
     * @param keywords
     * @return
     * @throws Exception
     */
    public static String getHighlighter(String content, Integer contentLength, List<String> keywords, String prefix,
        String suffix) throws Exception {
        String hightcontent = getHighlighter(content, contentLength, keywords, true, true);
        if (StringUtils.isEmpty(hightcontent)) {
            hightcontent = getHighlighter(content, contentLength, keywords, true, false);
        }
        return setHighHlight(hightcontent, keywords, prefix, suffix);
    }

    /**
     * 使用 cjk 分词器计算摘要信息
     *
     * @param content
     * @param contentLength
     * @param keywords
     * @return
     */
    public static String getHighlighterByCjk(String content, Integer contentLength, List<String> keywords)
        throws Exception {
        return getHighlighterByCjk(content, contentLength, keywords, null, null);
    }

    /**
     * 使用 cjk 分词器计算摘要信息
     *
     * @param content
     * @param contentLength
     * @param keywords
     * @return
     */
    public static String getHighlighterByCjk(String content, Integer contentLength, List<String> keywords,
        String prefix, String suffix) throws Exception {
        String hightcontent = getHighlighter(content, contentLength, keywords, false, false);
        return setHighHlight(hightcontent, keywords, prefix, suffix);
    }

    public static String setHighHlight(String hightcontent, List<String> keywords, String prefix, String suffix) {
        if (StringUtils.isEmpty(prefix) || StringUtils.isEmpty(suffix)) {
            prefix = PREFIX_TAG;
            suffix = SUFFIX_TAG;
        }
        if (keywords != null && keywords.size() > 0) {
            for (String keyword : keywords) {
                hightcontent = StringUtils.replace(hightcontent, keyword, prefix + keyword + suffix);
            }
        }
        return hightcontent;
    }

}