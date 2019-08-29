import java.util.ArrayList;
import java.util.List;

import com.codingme.eshelper.summary.SummaryMain;
import com.codingme.eshelper.summary.SummaryUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SummaryMainTest {

    /**
     * 测试案例
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        //MyUtils.init();
        String content =
            "，你好端午节：你农历五<b>月</b>初五习俗：雄黄酒、挂香袋、吃粽子、赛龙舟、挂艾草和菖蒲、斗百草、驱“五毒”等。</br></br> </br></br></br>浣溪沙</br></br>【宋】苏轼</br></br>轻汗微微透碧纨。明朝端午浴芳兰。</br></br>流香涨腻满晴川。彩线轻缠红玉臂。</br></br> </br> </br></br>6.七夕节：农历七月初七</br></br>习俗：穿针乞巧。</br></br> </br></br></br>秋夕</br></br>【唐】杜牧</br></br>银烛秋光冷画屏，轻罗小扇扑流萤。天阶夜色凉如水，卧看牵牛织女星。</br></br></br></br></br>7.中秋节：农历八月十五</br></br>习俗：赏月、祭月、观潮、吃月饼。</br></br> </br></br></br>月下独酌</br></br>李白花间一壶酒，独酌无相亲。举杯邀明月，对影成三人。月既不解饮，影徒随我身。暂伴月将影，行乐须及春";

        //String content = "转发微博";
        // 关键词转List
        List<String> list = SummaryUtils.keywordToList("龙舟+(苏轼|你好)");
        //log.info(list.toString());
        //ArrayList list = new ArrayList();
        //list.add("哈哈哈");
        // 使用 IK 分词器，默认高亮标签获取摘要
        String summary = SummaryMain.getSummary(content, 40, list);
        log.info("摘要:"+summary);
        log.info("----------------------------------------");

        // 使用 IK 分词器，默认高亮标签获取摘要
        summary = SummaryMain.getSummary(content, 500, list);
        log.info("摘要:"+summary);
        log.info("----------------------------------------");

        // 使用 IK 分词器，自定义高亮标签获取摘要
        String summary1 = SummaryMain.getSummary(content, 100, list, "【【", "】】");
        log.info("摘要:"+summary1);
        log.info("----------------------------------------");

        // 使用 CJK 分词器，默认高亮标签获取摘要
        String summary3 = SummaryMain.getSummaryByCjk(content, 100, list);
        log.info("摘要:"+summary3);
        log.info("----------------------------------------");

        // 使用 CJK 分词器，不要高亮获取摘要
        String summary4 = SummaryMain.getSummaryByCjk(content, 100, list, null, null);
        log.info("摘要:"+summary4);
        log.info("----------------------------------------");
    }

}