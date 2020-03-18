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

public class Haodaifu {
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, "jiankangdangan", "jiankangdangan_user", "shanzhen@2020");
    //    private static final MongoEntityTable<MongoJsonEntity> listTable = mongo.getJsonTable(database, "Haodaifu-List");
    private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "HaodaifuJibing-Brief");
    private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "HaodaifuJibing-Detail");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "HaodaifuJibing-Parsed");
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
        Progress progress = new Progress("detail", briefTable.count(null), 5000);
        for (MongoBytesEntity bytesEntity : detailTable.find(null)) {
            haodaifu.parseDetail(bytesEntity);
            progress.increase(1);
        }

    }

    private void grabBrief() {
        Document document = client.tryGet("https://www.haodf.com/jibing/list.htm", -1, HttpContent::toDocument, d -> !d.select("#m_body_html").isEmpty());
        Progress progress = new Progress("page", document.select(".kstl").size(), 30000);
        for (Element div : document.select(".kstl")) {
            String url = div.select("> a").attr("abs:href");
            String categoryName = div.select("> a").text();
            String[] index = url.split("/");
            String categoryId = index[index.length - 2];
            Document categoryList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select("#m_body_html").isEmpty());
            if (categoryList.select(".ksbd").isEmpty()) {
                for (Element a : categoryList.select(".m_ctt_green a")) {
                    url = a.attr("abs:href");
                    grabCategory(categoryId, categoryName, url);
                }
                //记录
                System.out.println(categoryName);
            } else {
                for (Element li : categoryList.select(".ksbd li")) {
                    url = li.select("> a").attr("abs:href");
                    index = url.split("/");
                    categoryId = index[index.length - 2];
                    categoryName = li.select("> a").text();
                    Document categoryDetailList = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".m_ctt_green a").isEmpty());
                    for (Element a : categoryDetailList.select(".m_ctt_green a")) {
                        url = a.attr("abs:href");
                        grabCategory(categoryId, categoryName, url);
                    }
                    //记录
                    System.out.println(categoryName);
                }

            }
            progress.increase(1);
        }
    }

    private void grabCategory(String categoryId, String categoryName, String prefix) {
        Document document = client.tryGet(prefix, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        JSONObject mateData = new JSONObject().fluentPut("name", categoryName).fluentPut("id", categoryId);
        String id = prefix.replace("https://www.haodf.com/jibing/", "").replace(".htm", "");
        JSONObject content = new JSONObject();
        String name = document.select(".s-cur > a").text();
        content.put("name", name);
        String title = document.select(".dis-title").text();
        content.put("title", title);
        Elements p = document.select(".dis-obj");
        String introduction = p.text().replace("全文阅读>", "");
        content.put("introduction", introduction);
        String url = prefix.replace(".htm", "/jieshao.htm");
        content.put("url", url);
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

        String title = document.select("h1").text();
        json.put("title", title);
        String description = document.select(".hot_recommend > p").text();
        json.put("description", description);

        Elements content = document.select(".recommend_main");
        if (!content.isEmpty()) {
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
        String url = (String) entity.getJsonMetadata().get("url");
        getArticle(url, json);
        getTopic(url, json);
        parsedTable.save(new MongoJsonEntity(entity.getId(), json, entity.getJsonMetadata()));
    }

    private void getTopic(String url, JSONObject json) {
        String topicUrl = url.replace("jieshao.htm", "topic_0_1.htm");
        Document topicList = client.tryGet(topicUrl, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        int totalPage = 1;
        if (!topicList.select(".page_turn font").isEmpty()) {
            totalPage = Integer.valueOf(topicList.select(".page_turn font").text());
        }
        System.out.println(json.getString("title") + " [topic] " + totalPage);
        List<JSONObject> topicJsonList = new ArrayList<>();
        json.put("topic", topicJsonList);
        for (int i = 1; i <= totalPage; ++i) {
            for (Element element : topicList.select(".kart_li")) {
                String title = element.select(".bb_e5 .kart_title").text();
                String label = element.select(".bb_e5 .kart_label1").text();
                String publishedTime = element.select(".bb_e5 .fr").text().replace("发布时间：", "");

                String content = element.select(".kart_con").text().replace("查看全文>>", "");
                String DetailUrl = element.select(".kart_con a").attr("abs:href");
                JSONObject topic = new JSONObject();
                topic.fluentPut("title", title)
                        .fluentPut("label", label)
                        .fluentPut("published_time", publishedTime)
                        .fluentPut("content", content)
                        .fluentPut("DetailUrl", DetailUrl);
                topicJsonList.add(topic);
            }
            topicUrl = topicUrl.replace(i + ".htm", (i + 1) + ".htm");
            topicList = client.tryGet(topicUrl, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        }
    }

    private void getArticle(String url, JSONObject json) {
        String articleUrl = url.replace("jieshao.htm", "article_0_1.htm");
        Document articleList = client.tryGet(articleUrl, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        int totalPage = 1;

        if (!articleList.select(".page_turn font").isEmpty()) {
            totalPage = Integer.valueOf(articleList.select(".page_turn font").text());
        }
        System.out.println(json.getString("title") + " [article] " + totalPage);
        List<JSONObject> articleJsonList = new ArrayList<>();
        json.put("article", articleJsonList);

        for (int i = 1; i <= totalPage; ++i) {
            for (Element element : articleList.select(".dis_article")) {
                String title = element.select(".article_title a").text();
                String publishedTime = element.select(".article_title span").text().replace("发表时间: ", "");
                Element a = element.select(".docmsg-right > p > a").first();
                String author = a.text();
                String position = a.nextElementSibling().text();
                String hospital = a.nextElementSibling().nextElementSibling().text();
                String department = a.nextElementSibling().nextElementSibling().nextElementSibling().text();
                String content = element.select(".con").text().replace("查看全文>>", "");
                String DetailUrl = element.select(".con a").attr("abs:href");
                String id = DetailUrl.replace("https://www.haodf.com/zhuanjiaguandian/", "").replace(".htm", "");
                JSONObject article = new JSONObject();
                article.fluentPut("id", id)
                        .fluentPut("title", title)
                        .fluentPut("author", author)
                        .fluentPut("published_time", publishedTime)
                        .fluentPut("position", position)
                        .fluentPut("hospital", hospital)
                        .fluentPut("department", department)
                        .fluentPut("content", content)
                        .fluentPut("DetailUrl", DetailUrl);
                articleJsonList.add(article);
            }
            articleUrl = articleUrl.replace(i + ".htm", (i + 1) + ".htm");
            articleList = client.tryGet(articleUrl, -1, HttpContent::toDocument, d -> !d.select(".d-nav-con .s-cur").isEmpty());
        }
    }
}
