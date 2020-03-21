package grab.weiyiguahao;


import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

import com.wayue.olympus.common.Progress;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class WeiyiGuahao {
    private static final Pattern PatternGroup = Pattern.compile("\\Qhttps://dxy.com/diseases/\\E(?<group>\\w+)/?");
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, "jiankangdangan", "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "Weiyiguahao-Parsed");


    private static final MongoClient mongoClient = mongo.getMongo();

    public static void main(String[] args) {
        WeiyiGuahao weiyiGuahao = new WeiyiGuahao();
        MongoCollection<org.bson.Document> collection = mongoClient.getDatabase(database).getCollection("weiyiguahao");
        Progress progress = new Progress("detail->parsed", collection.countDocuments(), 5000);
        for (org.bson.Document entity : collection.find()) {
            weiyiGuahao.parsedDetail(entity);
            progress.increase(1);
        }
    }

    private void parsedDetail(org.bson.Document entity) {
        Document document = Jsoup.parse((String) entity.get("html"));
        if (document.select(".department-page").isEmpty()) {
            return;
        }
        JSONObject json = new JSONObject();
        {//抓取基本信息
            Elements base = document.select(".el-card__body");
            String title = base.select("h1").text();
            String description = base.select(".bhc-body1").text();
            List<JSONObject> specificList = new ArrayList<>();
            json.put("title", title);
            json.put("description", description);
            json.put("specific_list", specificList);
            for (Element element : base.select(".list-component-content .el-col-6")) {
                Elements a = element.select("a");
                String name = a.text();
                //从数据库获取的网页信息没有超链接部分（只能提取href）
                String id = a.attr("href");
                String url = "https://baike.guahao.com" + id;
                JSONObject item = new JSONObject()
                        .fluentPut("id", id)
                        .fluentPut("name", name)
                        .fluentPut("url", url);
                specificList.add(item);
            }
        }
        {//文章信息，因为文章列表不能跳转，所以只保存第一页文章列表
            Elements list = document.select(".list-component1");
            List<JSONObject> articleList = new ArrayList<>();
            json.put("articles", articleList);
            for (Element element : list.select(".list-item-main")) {
                String tag = element.select(".list-item-main--title a span").text();
                String title = element.select(".list-item-main--title a").text().replace(tag, "");
                String url = "https://baike.guahao.com/" + element.select(".list-item-main--title a").attr("href");
                String description = element.select(".list-item-main--description span").text().replace("[详情]", "");

                String[] split = url.split("/");
                String id = split[split.length - 1].replace(".html", "");
                if (!id.matches("\\d+")) {
                    System.out.println(title + " wrong format url:" + url + " " + id);
                    continue;
                }
                JSONObject article = new JSONObject()
                        .fluentPut("id", id)
                        .fluentPut("title", title)
                        .fluentPut("tag", tag)
                        .fluentPut("description", description)
                        .fluentPut("url", url);
                articleList.add(article);
            }
        }
        String url = (String) entity.get("url");
        String id = url.replace("https://baike.guahao.com", "");
        parsedTable.save(new MongoJsonEntity(id, json));
        parsedTable.addToMetadata(id, "url", entity.get("url"));
    }

}
