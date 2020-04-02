package grab.weiyiguahao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wayue.olympus.common.Progress;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.mongo.*;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Guahao {
    private static final Pattern PatternDetailUrl = Pattern.compile("\\Qhttps://wedic.guahao.com/word/\\E(?<id>\\d+)");

    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "Guahao-Brief");
    private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "Guahao-Detail");
    private static final MongoEntityTable<MongoJsonEntity> parseTable1 = mongo.getJsonTable(database, "Guahao-Parsed-1");
    private static final MongoEntityTable<MongoJsonEntity> parseTable2 = mongo.getJsonTable(database, "Guahao-Parsed-2");

    private HttpClient client = new HttpClient();

    public static void main(String[] args) {
//        new Thread(() -> new Guahao().grabBrief(1)).start();
//        new Thread(() -> new Guahao().grabBrief(2)).start();
//        new Thread(() -> new Guahao().grabBrief(3)).start();
//        new Thread(() -> new Guahao().grabBrief(4)).start();
//        new Thread(() -> new Guahao().grabBrief(5)).start();
//        new Thread(() -> new Guahao().grabBrief(6)).start();
//        new Thread(() -> new Guahao().grabBrief(7)).start();
//        new Thread(() -> new Guahao().grabBrief(8)).start();
//        new Thread(() -> new Guahao().grabBrief(9)).start();
//        new Thread(() -> new Guahao().grabBrief(10)).start();
//        new Thread(() -> new Guahao().grabBrief(11)).start();
//        new Thread(() -> new Guahao().grabBrief(12)).start();
//        new Thread(() -> new Guahao().grabBrief(13)).start();
//        new Thread(() -> new Guahao().grabBrief(14)).start();
//        new Thread(() -> new Guahao().grabBrief(15)).start();
//        new Thread(() -> new Guahao().grabBrief(16)).start();
//        new Thread(() -> new Guahao().grabBrief(17)).start();
//        new Thread(() -> new Guahao().grabBrief(18)).start();
//        new Thread(() -> new Guahao().grabBrief(19)).start();
//        new Thread(() -> new Guahao().grabBrief(20)).start();
        Guahao guahao = new Guahao();
//        Progress progress = new Progress("brief", briefTable.count(null), 5000);
//        for (MongoJsonEntity entity : briefTable.find(null)) {
//            guahao.grabDetail(entity);
//            progress.increase(1);
//        }
        Progress progress = new Progress("detail", detailTable.count(null), 5000);
        detailTable.find(null).forEach(entity -> {
            guahao.parseDetail(entity);
            progress.increase(1);
        });
    }

    private void grabBrief(int num) {//使用多线程进行采集
        String prefix = "https://www.guahao.com/disease/";
        Progress progress = new Progress("thread-" + (num - 1), 5000, 5000);
        for (int i = num; i < 100000; i += 20) {
            if (briefTable.exists(String.valueOf(i))) {
                continue;
            }
            String url = prefix + i;
            Document document = client.tryGet(url, -1, HttpContent::toDocument, null);
            if (!document.select(".disease").isEmpty()) {
                Elements element = document.select(".disease.nobanner.J_NoBanner");
                String title = element.select("h1").text();
                JSONObject object = new JSONObject().fluentPut("title", title);
                for (Element li : element.select(" ul > li")) {
                    Elements span = li.select("> span");
                    String name = span.text().replace("：", "");
                    String text = li.text().replace(span.text(), "");
                    object.put(name, text);
                }
                Elements a = element.select("> a"), remove = a.select("span"), b = a.select("b");
                String summary = a.text().replace(remove.text(), "").replace(b.text(), "");
                String detailUrl = a.attr("abs:href");
                object.fluentPut("summary", summary).fluentPut("detailUrl", detailUrl);
                briefTable.save(new MongoJsonEntity(String.valueOf(i), object));
                briefTable.addToMetadata(String.valueOf(i), "category", new JSONObject().fluentPut("name", object.get("科室")));
            } else if (document.select(".msg").isEmpty()) {
                System.out.println(i);
            }
            progress.increase(1);
        }
    }

    private void grabDetail(String id) {
        String prefix = "https://www.guahao.com/disease/detail/";
        String url = prefix + id;
        Document document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".content").isEmpty());
        Elements select = document.select("#g-cfg");
        select.select(".g-grid10-r").remove();
        detailTable.save(new MongoBytesEntity(id, select.html().getBytes(), new JSONObject().fluentPut("url", url)));
    }

    private void grabDetail(MongoJsonEntity entity) {
        grabDetail(entity.getId());
        String url = entity.getJsonContent().getString("detailUrl");
        Matcher matcher = PatternDetailUrl.matcher(url);
        if (!matcher.find()) {
            return;
        }
        String id = matcher.group("id");
        Document document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".gp-container").isEmpty());
        Elements select = document.select(".gp-container");
        detailTable.save(new MongoBytesEntity(id, select.html().getBytes(), new JSONObject().fluentPut("url", url)));
    }

    private void parseDetail(MongoBytesEntity entity) {
        Document document = Jsoup.parse(new String(entity.getBytesContent()));
        JSONObject object = new JSONObject();

        if (document.select(".gp-right-bar").isEmpty()) {//https://www.guahao.com/disease/detail/37394
            parseDetail1(document, object);
            parseTable1.save(new MongoJsonEntity(entity.getId(), object, entity.getJsonMetadata()));
        } else {                                             //https://wedic.guahao.com/word/69516
            parseDetail2(document, object);
            parseTable2.save(new MongoJsonEntity(entity.getId(), object, entity.getJsonMetadata()));
        }
    }

    private void parseDetail1(Document document, JSONObject object) {
        String title = document.select(".nav > h1").text();
        object.put("title", title);
        Map<String, String> map = new HashMap<>();
        document.select(".nav-list > a").forEach(a -> map.put(a.text(), a.attr("href").replace("#", "")));

        for (Element element : document.select(".content > .content-box")) {
            String name = map.get(element.select("> h3").text());
            Elements div = element.select("> div");
            String text = null;
            if (!div.select(".info").isEmpty()) {
                JSONObject basic = new JSONObject();
                for (Element li : div.select("li")) {
                    Elements span = li.select("> span");
                    String t = span.text().replace("：", "");
                    text = li.text().replace(span.text(), "");
                    basic.fluentPut(t, text);
                }
                object.put(name, basic);
                continue;
            } else if (!div.select(".moreContent").isEmpty()) {
                String remove = StringUtils.trim(div.select(".moreContent > span").text());
                text = div.select(".moreContent").text().replace(remove, "");
            } else {
                text = div.text();
            }
            object.put(name, text);
        }
    }

    private void parseDetail2(Document document, JSONObject object) {
        String title = document.select(".gp-main-title > h1").text();
        String summary = document.select(".gp-introduce").text();
        JSONObject basic = new JSONObject();
        object.fluentPut("title", title).fluentPut("summary", summary).fluentPut("basic", basic);
        for (Element element : document.select(".basic-info > .left-part")) {
            String name = element.select(".name").text().replace("：", "");
            String text = element.select(".value").text();
            basic.put(name, text);
        }
        Map<String, String> map = new HashMap<>();
        map.put("疾病知识", "knowledge");
        map.put("健康问答", "qa");
        map.put("科普文章", "articles");
        map.put("参考资料", "references");
        map.put("词条标签", "tags");
        map.put("外部链接", "externalLink");

        for (Element element : document.select(".gp-card-part")) {
            String partTitle = map.get(element.select("> h1").text());
            if (partTitle == null) {
                System.out.println(title + "---" + element.select("> h1").text());
                continue;
            }
            JSONArray content = new JSONArray();
            parsePart(element, partTitle, content);
            object.put(partTitle, content);
        }

    }

    private void parsePart(Element element, String title, JSONArray list) {
        JSONObject content = null;
        switch (title) {
            case "knowledge": {
                for (Element div : element.select(".gp-card-module")) {
                    content = new JSONObject().fluentPut("title", div.select("> h2").text())
                            .fluentPut("text", div.select(".content").text());
                    list.add(content);
                }
                break;
            }
            case "qa": {
                for (Element box : element.select(".gp-qa-box")) {
                    content = new JSONObject()
                            .fluentPut("question", box.select(".gp-queastion").text())
                            .fluentPut("answer", box.select(".detail").text()
                                    .replace("下拉查看详情", "")
                                    .replace("...", ""))
                            .fluentPut("answerer", new JSONObject()
                                    .fluentPut("avatarUrl", box.select("img").attr("src"))
                                    .fluentPut("name", box.select(".name").text())
                                    .fluentPut("position", box.select(".title > span").first().text())
                                    .fluentPut("hospital", box.select(".title > span").last().text()));
                    list.add(content);
                }
                break;
            }
            case "articles": {
                for (Element box : element.select(".gp-article-box")) {
                    content = new JSONObject();
                    if (box.hasClass("article-author-info")) {
                        content.put("author", new JSONObject()
                                .fluentPut("avatarUrl", box.select(".article-author-info > img").attr("src"))
                                .fluentPut("name", box.select(".doc-info > span").first().text())
                                .fluentPut("position", box.select(".doc-info > span").last().text())
                                .fluentPut("hospital", box.select(".doc-info > p").text().split(" ")[0])
                                .fluentPut("department", box.select(".doc-info > p").text().split(" ")[1]));
                    }
                    content.fluentPut("title", box.select(".article-detail > h1").text())
                            .fluentPut("summary", box.select(".article-detail > p").text())
                            .fluentPut("detailUrl", box.select("> a").first().attr("abs:href"));
                    list.add(content);
                }
                break;
            }
            case "references": {
                for (Element p : element.select(".content > p")) {
                    content = new JSONObject().fluentPut("text", p.text());
                    list.add(content);
                }
                break;
            }
            case "tags": {
                for (Element a : element.select(".content a")) {
                    String prefix = "https://wedic.guahao.com/word/";
                    content = new JSONObject()
                            .fluentPut("title", a.text())
                            .fluentPut("url", prefix + a.attr("href"));
                    list.add(content);
                }
                break;
            }
            case "externalLink": {
                for (Element a : element.select(".content a")) {
                    content = new JSONObject()
                            .fluentPut("title", a.text())
                            .fluentPut("url", a.attr("href"));
                    list.add(content);
                }
                break;
            }
        }
    }
}
