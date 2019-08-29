package com.codingme.eshelper.summary;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * 取摘要
 *
 * @Author niujinpeng
 * @Date 2019/6/28 10:20
 */
public class SummaryUtils {

	/** 摘要满分 */
	private static final double FULL_SCORE = 100;
	/** 摘要中关键词最高得分 */
	private static final double KEYWORD_ALL_SCORE = 50;
	/** 摘要最大长度 */
	private static final double SUMMARY_MAX_LENGTH = 400;
	/** 摘要最短长度 */
	private static final double SUMMARY_MIN_LENGTH = 0;
	/** 摘要最优长度 */
	private static final double SUMMARY_AVG_LENGTH = (SUMMARY_MAX_LENGTH + SUMMARY_MIN_LENGTH) / 2;
	/** 摘要中距离最优长度的每个差值的平均减分 */
	private static final double LENGTH_AVG_SCORE = (FULL_SCORE - KEYWORD_ALL_SCORE)
			/ (SUMMARY_AVG_LENGTH - SUMMARY_MIN_LENGTH);
	/** 不会被清理的开头特殊符号 */
	private static HashSet<String> NORMAL_SYMBOL_START_SET = new HashSet<>();
	private static final Pattern P_HTML = Pattern.compile("<[^>]+>", Pattern.CASE_INSENSITIVE);
	/** 所有标点符号 */
	private static final HashSet<String> symbolAllSet = new HashSet<>();
	/** 表示结束的标点符号 */
	private static final HashSet<String> symbolEndSet = new HashSet<>();
	/** 需要保留的HTML标点符号 */
	private static final HashSet<String> symbolHtmlSet = new HashSet<>();
	/** 表示停顿的标点符号 */
	private static final HashSet<String> symbolMiddleSet = new HashSet<>();

	static {
		symbolEndSet.add("。");
		symbolEndSet.add("？");
		symbolEndSet.add("?");
		symbolEndSet.add("！");
		symbolEndSet.add("!");
		symbolEndSet.add("<p>");
		symbolEndSet.add("</p>");

		symbolMiddleSet.add(";");
		symbolMiddleSet.add("；");
		symbolMiddleSet.add("，");
		symbolMiddleSet.add(",");
		symbolMiddleSet.add("：");
		symbolMiddleSet.add(":");
		symbolMiddleSet.add("……");
		symbolMiddleSet.add("</br>");
		symbolMiddleSet.add("<br/>");

		symbolHtmlSet.add("<p>");
		symbolHtmlSet.add("</p>");
		symbolHtmlSet.add("</br>");
		symbolHtmlSet.add("<br/>");

		symbolAllSet.addAll(symbolMiddleSet);
		symbolAllSet.addAll(symbolEndSet);

		NORMAL_SYMBOL_START_SET.add("《");
		NORMAL_SYMBOL_START_SET.add("<");
		NORMAL_SYMBOL_START_SET.add("（");
		NORMAL_SYMBOL_START_SET.add("(");
		NORMAL_SYMBOL_START_SET.add("“");
		NORMAL_SYMBOL_START_SET.add("\"");
		NORMAL_SYMBOL_START_SET.add("【");
		NORMAL_SYMBOL_START_SET.add("[");
	}

	/**
	 * 检查指定字符串中是否包含指定集合中某个数值
	 *
	 * @param content
	 * @param symbolSet
	 * @return
	 */
	public static boolean checkHasSymbol(String content, HashSet<String> symbolSet) {
		for (String symbol : symbolSet) {
			if (StringUtils.contains(content, symbol)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 检查字符是否是特殊字符
	 *
	 * @param str
	 * @return
	 */
	public static boolean checkIsSymbol(String str) {
		if (str == null || str.length() != 1) {
			return false;
		}
		char ch = str.charAt(0);
		int chInt = ch;
		// 检查是否是中文,数字，字母
		if ((chInt >= 19968 && chInt <= 40869) || (chInt >= '0' && chInt <= '9') || (chInt >= 'a' && chInt <= 'z')
				|| (chInt >= 'A' && chInt <= 'Z')) {
			return false;
		}
		return true;
	}

	public static String clearHtml(String con) {
		if (con == null) {
			return con;
		}
		con = con.replace("&nbsp;", StringUtils.EMPTY);
		con = con.replace("\r\n", StringUtils.EMPTY);
		Matcher mHtml = P_HTML.matcher(con);
		con = mHtml.replaceAll(" ");
		int k = con.indexOf("&nbsp;");
		while (k == 0) {
			con = con.substring(6);
			k = con.indexOf("&nbsp;");
		}
		return con;
	}

	/**
	 * 去除特殊符号
	 *
	 * @param summary
	 * @return
	 */
	public static String clearSymbol(String summary) {
		// 处理特殊HTML符号
		for (String html : symbolHtmlSet) {
			if (summary.contains(html)) {
				summary = StringUtils.replace(summary, html, StringUtils.EMPTY);
			}
		}
		// 处理开头无意义符号
		String param = StringUtils.substring(summary, 0, 1);
		while (checkIsSymbol(param)) {
			if (!NORMAL_SYMBOL_START_SET.contains(param)) {
				summary = StringUtils.substring(summary, 1);
				param = StringUtils.substring(summary, 0, 1);
			} else {
				param = null;
			}
		}
		// 处理结尾无意义符号
		param = StringUtils.substring(summary, summary.length() - 1);
		while (checkIsSymbol(param)) {
			if (StringUtils.equals("<", param)) {
				summary = StringUtils.substring(summary, 0, summary.length() - 1);
				param = StringUtils.substring(summary, summary.length() - 1);
			} else {
				param = null;
			}
		}
		return summary;

	}

	/**
	 * 定位所有匹配位置
	 *
	 * @param summary
	 * @param symbolSet
	 * @return
	 */
	public static ArrayList<Integer> findAllMatchIndex(String summary, HashSet<String> symbolSet) {
		ArrayList<Integer> indexList = new ArrayList<>();
		char[] chars = summary.toCharArray();
		for (int i = 0; i < chars.length; i++) {
			char aChar = chars[i];
			if (symbolSet.contains(String.valueOf(aChar))) {
				indexList.add(i);
			}
		}
		for (String s : symbolSet) {
			if (s.length() > 1) {
				indexList.addAll(findAllMatchIndex(summary, s));
			}
		}
		indexList.sort(Comparator.comparing(Integer::intValue));
		return indexList;
	}

	/**
	 * 定位所有匹配位置
	 *
	 * @param summary
	 * @param symbol
	 * @return
	 */
	public static ArrayList<Integer> findAllMatchIndex(String summary, String symbol) {
		ArrayList<Integer> indexList = new ArrayList<>();
		int index = -1;
		while ((index = StringUtils.indexOf(summary, symbol, index)) != -1) {
			indexList.add(index);
			index += symbol.length();
		}
		return indexList;
	}

	/**
	 * 寻找所有匹配的区间
	 *
	 * @param summaryIndexSet
	 * @param symbolEndIndexList
	 * @return
	 */
	public static LinkedHashSet<SummaryIndex> findAllMatchRedAndSybmol(HashSet<SummaryIndex> summaryIndexSet,
																	   ArrayList<Integer> symbolEndIndexList) {
		LinkedHashSet<SummaryIndex> summaryIndexAllSet = new LinkedHashSet<>();
		for (SummaryIndex summaryIndex : summaryIndexSet) {
			int startListIndex = summaryIndex.getStartListIndex();
			while (startListIndex >= 0) {
				int startChar = symbolEndIndexList.get(startListIndex);
				int endListIndex = summaryIndex.getEndListIndex();
				while (endListIndex < symbolEndIndexList.size()) {
					Integer endChar = symbolEndIndexList.get(endListIndex);
					SummaryIndex summaryIndexTemp = new SummaryIndex(startChar, endChar, startListIndex, endListIndex);
					summaryIndexAllSet.add(summaryIndexTemp);
					endListIndex++;
				}
				startListIndex--;
			}
		}
		return summaryIndexAllSet;
	}

	/**
	 * 寻找最优摘要
	 *
	 * @param summaryIndexAllSet
	 * @param colorIndexList
	 * @return
	 */
	public static SummaryIndex findGoodContentIndex(LinkedHashSet<SummaryIndex> summaryIndexAllSet,
			ArrayList<Integer> colorIndexList, double max, double min, double avgLength, double avgScore, String prefix,
			String suffix) {
		double colorScore = KEYWORD_ALL_SCORE / colorIndexList.size();
		SummaryIndex goodSummaryIndex = null;
		for (SummaryIndex summaryIndex : summaryIndexAllSet) {
			double score = 0;
			int startCharIndex = summaryIndex.getStartCharIndex();
			int endCharIndex = summaryIndex.getEndCharIndex();
			int length = summaryIndex.getLength();
			for (Integer color : colorIndexList) {
				if (startCharIndex < color && color < endCharIndex) {
					score = score + colorScore;
					length = length - prefix.length() - suffix.length();
				}
			}
			if (length < min || length > max) {
				continue;
			}
			double keywordScore = (FULL_SCORE - KEYWORD_ALL_SCORE) - Math.abs(length - avgLength) * avgScore;
			score += keywordScore;
			summaryIndex.setScore(score);
			if (goodSummaryIndex == null || score > goodSummaryIndex.getScore()) {
				goodSummaryIndex = summaryIndex;
			}
		}
		return goodSummaryIndex;
	}

	/**
	 * 寻找基础的匹配区间
	 *
	 * @param colorIndexList
	 * @param symbolEndIndexList
	 * @return
	 */
	public static HashSet<SummaryIndex> findMatchRedAndSymbol(ArrayList<Integer> colorIndexList,
			ArrayList<Integer> symbolEndIndexList, double max, String prefix, String suffix) {
		HashSet<SummaryIndex> summaryIndexSet = new HashSet<>();
		if (CollectionUtils.isEmpty(symbolEndIndexList) || symbolEndIndexList.size() == 1) {
			return summaryIndexSet;
		}
		for (Integer colorIndex : colorIndexList) {
			for (int i = 1; i < symbolEndIndexList.size(); i++) {
				Integer endIndex = symbolEndIndexList.get(i);
				Integer start = symbolEndIndexList.get(i - 1);
				if (start > colorIndex) {
					break;
				}
				if (endIndex > colorIndex && start <= colorIndex) {
					SummaryIndex summaryIndex = new SummaryIndex(start, endIndex, i - 1, i);
					int length = summaryIndex.getLength() - prefix.length() - suffix.length();
					if (length > max) {
						continue;
					}
					summaryIndex.setLength(length);
					summaryIndexSet.add(summaryIndex);
				}
			}
		}
		return summaryIndexSet;
	}

	/**
	 * 由内至外寻找最优摘要
	 *
	 * @param content
	 * @return
	 */
	public static String findSummary(String content, Integer optimalSummaryLength, String prefix, String suffix) {
		double max = SUMMARY_MAX_LENGTH;
		double min = SUMMARY_MIN_LENGTH;
		double avgLength = SUMMARY_AVG_LENGTH;
		double avgScore = LENGTH_AVG_SCORE;
		if (optimalSummaryLength == null) {
			optimalSummaryLength = LuceneHighlightUtils.CONTENT_LENGTH / 2;
		}
		if (optimalSummaryLength != null) {
			max = optimalSummaryLength * 2;
			avgLength = optimalSummaryLength;
			avgScore = (FULL_SCORE - KEYWORD_ALL_SCORE) / (avgLength - min);
		}
		if (StringUtils.isEmpty(prefix) || StringUtils.isEmpty(suffix)) {
			prefix = LuceneHighlightUtils.PREFIX_TAG;
			suffix = LuceneHighlightUtils.SUFFIX_TAG;
		}
		ArrayList<Integer> symbolEndIndexList = findAllMatchIndex(content, symbolEndSet);
		ArrayList<Integer> colorIndexList = findAllMatchIndex(content, prefix);
		if (CollectionUtils.isEmpty(colorIndexList)) {
			return StringUtils.EMPTY;
		}
		if (CollectionUtils.isNotEmpty(symbolEndIndexList)) {
			Integer symbolStart = symbolEndIndexList.get(0);
			Integer colorStart = colorIndexList.get(0);
			if (colorStart < symbolStart) {
				String subStrByEnd = StringUtils.substring(content, colorStart);
				if (!checkHasSymbol(subStrByEnd, symbolMiddleSet)) {
					/**
					 * 如果第一个结束符在关键词之后，且之前没有任何中断符号，补起始标识符<br/>
					 * PS: xxxxxxKEYxxxx。xxxxxx
					 */
					symbolEndIndexList.add(0);
				} else {
					/**
					 * 如果第一个结束符在关键词之后，且第一个中断符号在关键词之前，第一个中断符号补起始标识符<br/>
					 * PS: xxxxx，xxxxKEYxxxx。xxxxxx
					 */
					int indexHasSymbol = getIndexHasSymbol(content, symbolMiddleSet, true);
					if (colorStart > indexHasSymbol) {
						symbolEndIndexList.add(indexHasSymbol);
					}
				}
				symbolEndIndexList.sort(Comparator.comparing(Integer::intValue));
			}
		}
		// 寻找基础匹配区间
		HashSet<SummaryIndex> summaryIndexSet = findMatchRedAndSymbol(colorIndexList, symbolEndIndexList, max, prefix,
				suffix);
		if (CollectionUtils.isEmpty(summaryIndexSet)) {
			symbolEndIndexList = findAllMatchIndex(content, symbolAllSet);
			if (CollectionUtils.isNotEmpty(symbolEndIndexList)) {
				Integer symbolStart = symbolEndIndexList.get(0);
				Integer colorStart = colorIndexList.get(0);
				if (colorStart <= symbolStart) {
					symbolEndIndexList.add(0);
				}
				Integer symbolEnd = symbolEndIndexList.get(symbolEndIndexList.size() - 1);
				Integer colorEnd = colorIndexList.get(colorIndexList.size() - 1);
				if (symbolEnd < colorEnd) {
					symbolEndIndexList.add(content.length());
				}
			} else {
				symbolEndIndexList.add(0);
				symbolEndIndexList.add(content.length());
			}
			symbolEndIndexList.sort(Comparator.comparing(Integer::intValue));
			summaryIndexSet = findMatchRedAndSymbol(colorIndexList, symbolEndIndexList, max, prefix, suffix);
		}
		if (CollectionUtils.isEmpty(summaryIndexSet)) {
			return StringUtils.EMPTY;
		}
		// 寻找所有匹配区间
		LinkedHashSet<SummaryIndex> summaryIndexAllSet = findAllMatchRedAndSybmol(summaryIndexSet, symbolEndIndexList);
		// 寻找最优摘要
		SummaryIndex goodSummaryIndex = findGoodContentIndex(summaryIndexAllSet, colorIndexList, max, min, avgLength,
				avgScore, prefix, suffix);
		if (goodSummaryIndex == null) {
			return StringUtils.EMPTY;
		}
		int end = goodSummaryIndex.getEndCharIndex() + 1;
		if (end > content.length()) {
			end = content.length();
		}
		int start = goodSummaryIndex.getStartCharIndex();
		String summary = StringUtils.substring(content, start, end);
		summary = clearSymbol(summary);
		return summary;

	}

	/**
	 * 检查指定字符串中对于指定集合中值的首次或者末次出现位置
	 *
	 * @param content
	 * @param symbolSet
	 * @param isFirst   true 首次 / false 末次
	 * @return
	 */
	public static int getIndexHasSymbol(String content, HashSet<String> symbolSet, boolean isFirst) {
		int index = -1;
		for (String symbol : symbolSet) {
			int indexOf = -1;
			if (isFirst) {
				indexOf = StringUtils.indexOf(content, symbol);
			} else {
				indexOf = StringUtils.lastIndexOf(content, symbol);
			}
			if (indexOf == -1) {
				continue;
			}
			if (index == -1) {
				index = indexOf;
			} else {
				if (isFirst) {
					index = Math.min(index, indexOf);
				} else {
					index = Math.max(index, indexOf);
				}
			}
		}
		return index;
	}

	/**
	 * 关键词转List
	 *
	 * @param stream
	 * @return
	 * @throws Exception
	 */
	public static List<String> keywordToList(String stream) throws Exception {
		List<String> stringList = new ArrayList<String>();
		if (StringUtils.isNotEmpty(stream)) {
			String keywordTemp = stream;
			keywordTemp = StringUtils.replace(keywordTemp, "(", StringUtils.SPACE);
			keywordTemp = StringUtils.replace(keywordTemp, ")", StringUtils.SPACE);
			keywordTemp = StringUtils.replace(keywordTemp, "+", StringUtils.SPACE);
			keywordTemp = StringUtils.replace(keywordTemp, "|", StringUtils.SPACE);
			String[] keywords = keywordTemp.split(StringUtils.SPACE);
			if (keywords != null && keywords.length > 0) {
				for (String kwTemp : keywords) {
					if (StringUtils.isNotEmpty(kwTemp)) {
						stringList.add(kwTemp);
					}
				}
			}
		}
		return stringList;
	}



}