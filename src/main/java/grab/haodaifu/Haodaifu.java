package grab.haodaifu;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wayue.olympus.common.Progress;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.http.HttpUtil;
import com.wayue.olympus.common.mongo.MongoBytesEntity;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import org.apache.http.client.methods.RequestBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Haodaifu {
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, "jiankangdangan", "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "Haodaifu.disease-Brief");
    private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "Haodaifu.disease-Detail");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "Haodaifu.disease-Parsed");
    private static final MongoEntityTable<MongoJsonEntity> articleTable = mongo.getJsonTable(database, "Haodaifu.articles");
    private static final MongoEntityTable<MongoJsonEntity> topicTable = mongo.getJsonTable(database, "Haodaifu.topics");
//    private static final MongoEntityTable<MongoJsonEntity> standardTable = mongo.getJsonTable(database, "Haodaifu-Standard");

    private HttpClient client = new HttpClient();

    public static void main(String[] args) {
        Haodaifu haodaifu = new Haodaifu();

//        haodaifu.grabBrief();
//        Progress progress = new Progress("brief", briefTable.count(null), 5000);
//        for (MongoJsonEntity entity : briefTable.find(null)) {
//            haodaifu.grabDetail(entity);
//            progress.increase(1);
//        }
//        Progress progress = new Progress("detail", briefTable.count(null), 30000);
//        for (MongoBytesEntity bytesEntity : detailTable.find(null)) {
//            haodaifu.parseDetail(bytesEntity);
//            progress.increase(1);
//        }
        Progress progress = new Progress("brief->article", briefTable.count(null), 30000);
        for (MongoJsonEntity entity : briefTable.find(null)) {
            haodaifu.grabArticle(entity);
            progress.increase(1);
        }
//        Progress progress = new Progress("brief->topic", briefTable.count(null), 30000);
//        for (MongoJsonEntity entity : briefTable.find(null)) {
//            haodaifu.grabTopic(entity);
//            progress.increase(1);
//        }
    }


    private void grabBrief() {
        //访问频次过会导致快导致访问不了
        Document document = client.tryGet("https://www.haodf.com/jibing/list.htm", -1, HttpContent::toDocument, d -> !d.select("#m_body_html").isEmpty());
        Progress progress = new Progress("CategoryList", document.select(".kstl").size(), 30000);
        for (Element div : document.select(".kstl")) {//左侧边栏
            //从网址上获取大类id
            String url = div.select("> a").attr("abs:href");
            String[] index = url.split("/");
            String categoryId = index[index.length - 2];
            String categoryName = div.select("> a").text();

            Document categoryList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select("#m_body_html").isEmpty());
            //没有.ksbd(大类里面没有小类别的情况)
            if (categoryList.select(".ksbd").isEmpty()) {
                //从大类里获取每种疾病
                for (Element a : categoryList.select(".m_ctt_green a")) {
                    url = a.attr("abs:href");
                    grabCategory(categoryId, categoryName, url);
                }
                System.out.println(categoryName);
            } else {
                //大类里有小类
                for (Element li : categoryList.select(".ksbd li")) {
                    //从网址中回去小类id
                    url = li.select("> a").attr("abs:href");
                    index = url.split("/");
                    categoryId = index[index.length - 2];
                    categoryName = li.select("> a").text();

                    Document categoryDetailList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".m_ctt_green a").isEmpty());
                    //从小类里获取每种疾病
                    for (Element a : categoryDetailList.select(".m_ctt_green a")) {
                        url = a.attr("abs:href");
                        grabCategory(categoryId, categoryName, url);
                    }
                    System.out.println(categoryName);
                }
            }
            progress.increase(1);
        }
    }

    private void grabCategory(String categoryId, String categoryName, String prefix) {
        Document document = client.tryGet(prefix, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());

        JSONObject mateData = new JSONObject().fluentPut("name", categoryName).fluentPut("id", categoryId);
        JSONObject content = new JSONObject();
        String id = prefix.replace("https://www.haodf.com/jibing/", "").replace(".htm", "");
        {//提取简介内容
            String name = document.select(".s-cur > a").text();
            Elements p = document.select(".dis-obj");
            String introduction = p.text().replace("全文阅读>", "");
            String url = prefix.replace(".htm", "/jieshao.htm");

            content.put("name", name);
            content.put("introduction", introduction);
            content.put("url", url);
        }
        briefTable.save(new MongoJsonEntity(id, content));
        briefTable.addToMetadata(id, "tag", mateData);
    }

    private void grabDetail(MongoJsonEntity entity) {
        String id = entity.getId();
        String url = entity.getJsonContent().getString("url");
        Document document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        if (document == null) {
            return;
        }
        Elements elements = document.select(".d-nav-con, .dis_description, .liney");
        elements.select(".w320").remove();
        detailTable.save(new MongoBytesEntity(id, elements.html().getBytes(), new JSONObject().fluentPut("url", url)));
    }

    private void parseDetail(MongoBytesEntity entity) {
        Document document = Jsoup.parse(new String(entity.getBytesContent()));
        JSONObject json = new JSONObject();
        {//获取基本信息
            String title = document.select("h1").text();
            String description = document.select(".hot_recommend > p").text();
            json.put("title", title);
            json.put("description", description);
        }

        {//获取详细介绍
            Elements content = document.select(".recommend_main");
            List<JSONObject> paragraphs = new ArrayList<>();
            json.put("paragraphs", paragraphs);
            JSONObject paragraph = null;
            for (Element element : content) {
                paragraph = new JSONObject()
                        .fluentPut("title", element.select("> h2").text())
                        .fluentPut("texts", element.select(".js-longcontent").text());
                paragraphs.add(paragraph);
            }
        }
        parsedTable.save(new MongoJsonEntity(entity.getId(), json, entity.getJsonMetadata()));
    }

    private void grabTopic(MongoJsonEntity entity) {

        String url = "https://www.haodf.com/jibing/" + entity.getId() + "/topic_0_1.htm";
        Document topicList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        int totalPage = 1;
        if (!topicList.select(".page_turn font").isEmpty()) {
            totalPage = Integer.valueOf(topicList.select(".page_turn font").text());
        }
        String category = entity.getJsonContent().getString("name");

        for (int i = 1; i <= totalPage; ++i) {
            for (Element element : topicList.select(".kart_li")) {
                String title = element.select(".bb_e5 .kart_title").text();
                String label = element.select(".bb_e5 .kart_label1").text();
                String publishedTime = element.select(".bb_e5 .fr").text().replace("发布时间：", "");

                String content = element.select(".kart_con").text().replace("查看全文>>", "");
                String detailUrl = element.select(".kart_con a").attr("abs:href");
                String id = detailUrl.replace("https://www.haodf.com/paperdetail/", "")
                        .replace("https://www.haodf.com/zhuanjiaguandian/", "")
                        .replace("https://m.haodf.com/zhuanjiaguandian/", "").replace(".htm", "");

                JSONObject topic = new JSONObject();
                topic.fluentPut("title", title)
                        .fluentPut("label", label)
                        .fluentPut("published_time", publishedTime)
                        .fluentPut("content", content)
                        .fluentPut("detail_url", detailUrl);
                topicTable.save(new MongoJsonEntity(id, topic));
                topicTable.addToMetadata(id, "category", new JSONObject().fluentPut("name", category));
            }
            url = url.replace(i + ".htm", (i + 1) + ".htm");
            topicList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        }
    }

    private void grabArticle(MongoJsonEntity entity) {
        String url = "https://www.haodf.com/jibing/" + entity.getId() + "/article_0_1.htm";
        Document articleList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        int totalPage = 1;
        if (!articleList.select(".page_turn font").isEmpty()) {
            totalPage = Integer.valueOf(articleList.select(".page_turn font").text());
        }
        String category = entity.getJsonContent().getString("name");

        for (int i = 1; i <= totalPage; ++i) {
            for (Element element : articleList.select(".dis_article")) {
                String detailUrl = element.select(".con a").attr("abs:href");
                String id = detailUrl.replace("https://www.haodf.com/paperdetail/", "")
                        .replace("https://www.haodf.com/zhuanjiaguandian/", "")
                        .replace("https://m.haodf.com/zhuanjiaguandian/", "").replace(".htm", "");
                String title = element.select(".article_title a").text();
                JSONObject author = null;
                {//获取医生作者信息
                    Element p = element.select(".docmsg-right > p").first();
                    //目前发现网站一处有bug，有一页是空白的，没有内容,其余都没有问题
                    if (p == null) {continue;}
                    String name = p.select("> a").text();
                    String position = "";
                    String hospital = "";
                    String department = "";
                    Elements spans = p.select("> span");
                    if (spans.size() == 3) {//显示职位的医生作者
                        position = spans.get(0).text();
                        hospital = spans.get(1).text();
                        department = spans.get(2).text();
                    } else if (spans.size() == 2) {//不显示职位的医生作者
                        hospital = spans.get(0).text();
                        department = spans.get(1).text();
                    }
                    if (position.equals("") || hospital.equals("") || department.equals("")) {
                        System.out.println(category + " -page " + i + " -ti " + title + " -po " + position + " -ho " + hospital + " -de " + department);
                    }
                    author = new JSONObject().fluentPut("name", name)
                            .fluentPut("position", position)
                            .fluentPut("hospital", hospital)
                            .fluentPut("department", department);
                }
                String publishedTime = element.select(".article_title span").text().replace("发表时间: ", "");
                String content = element.select(".con").text().replace("查看全文>>", "");
                JSONObject article = new JSONObject().fluentPut("title", title)
                        .fluentPut("author", author)
                        .fluentPut("published_time", publishedTime)
                        .fluentPut("content", content)
                        .fluentPut("detail_url", detailUrl);
                articleTable.save(new MongoJsonEntity(id, article));
                articleTable.addToMetadata(id, "category", new JSONObject().fluentPut("name", category));
            }
            url = url.replace(i + ".htm", (i + 1) + ".htm");
            articleList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        }
    }
}
