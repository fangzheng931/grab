package grab.medlive;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.wayue.olympus.common.Progress;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

public class Medlive {
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "medlive-Parsed");

    private static final MongoClient mongoClient = mongo.getMongo();
    private HttpClient client = new HttpClient();

    public static void main(String[] args) {
        Medlive medlive = new Medlive();
        MongoCollection<org.bson.Document> collection = mongoClient.getDatabase(database).getCollection("medlive");
        Progress progress = new Progress("detail->parsed", collection.countDocuments(), 5000);
        for (org.bson.Document entity : collection.find()) {
            medlive.parsedDetail(entity);
            progress.increase(1);
        }
    }

    private void parsedDetail(org.bson.Document entity) {
        String prefix = "http://disease.medlive.cn/wiki/entry/";
        String url = (String) entity.get("url");
        if (!url.contains(prefix + "1")) {
            return;
        }
        Document document = Jsoup.parse((String) entity.get("html"));
        document.select(".sideBar").remove();
        String id = url.replace(prefix, "");
        String categoryId = id.split("_")[1];
        id = id.split("_")[0];
        if (parsedTable.exists(id)) {
            MongoJsonEntity jsonEntity = parsedTable.get(id);
            JSONObject jsonContent = jsonEntity.getJsonContent();
            addToContent(document, jsonContent, categoryId);
            parsedTable.save(new MongoJsonEntity(jsonEntity.getId(), jsonContent));
        } else {
            JSONObject jsonContent = new JSONObject();
            setBaseInfo(id, prefix, jsonContent);
            addToContent(document, jsonContent, categoryId);
            parsedTable.save(new MongoJsonEntity(id, jsonContent));
        }
    }

    private void setBaseInfo(String id, String prefix, JSONObject content) {
        String url = prefix + id + "_101_0";
        Document document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".main").isEmpty());

        {//设置名字简介
            Elements h3 = document.select(".knw_overview h3 > span");
            content.put("name", h3.text().replace(" -", ""));
            Elements summary = document.select(".summary");
            List<JSONObject> list = new ArrayList<>();
            content.put("introduction", list);
            JSONObject introduction;
            for (Element li : summary.select("li")) {
                introduction = new JSONObject().fluentPut("text", li.text());
                list.add(introduction);
            }
        }
        {//设置名称编码
            url = prefix + id + "_102_0";
            document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".main").isEmpty());
            List<JSONObject> list = new ArrayList<>();
            content.put("nameCoding", list);
            JSONObject nameCoding;
            Elements table = document.select(".nameCoding");
            for (Element li : table.select("li")) {
                String title = li.select("span").text().replace("：", "");
                String text = li.select("p").text();
                nameCoding = new JSONObject().fluentPut("title", title)
                        .fluentPut("text", text);
                list.add(nameCoding);
            }
        }
    }

    private void addToContent(Document document, JSONObject content, String categoryId) {
        System.out.println(content.get("name") + "--" + categoryId);
        switch (categoryId) {
            case "401":
            case "403":
            case "404":
            case "405":
                parseMore(document, content);
                break;
            case "407":
                parseTable(document, content);
                break;
            case "502":
                parseLink(document, content);
                break;
            default:
                parseSummary(document, content, categoryId);
                break;
        }


    }

    private void parseSummary(Document document, JSONObject content, String categoryId) {

        if (!document.select(".define").isEmpty()) {
            System.out.println(content.get("name") + "-define-" + categoryId);
            Elements div = document.select(".define");
            JSONObject define = new JSONObject();
            define.put("wiki_editor", div.select(".wiki_editor > a").text());
            List<JSONObject> list = new ArrayList<>();
            define.put("description", list);
            JSONObject description;
            for (Element element : div.select("> .descrip")) {
                description = new JSONObject().fluentPut("content", element.select("> div").text())
                        .fluentPut("source", element.select("> p").text().replace("来源：", ""));
                list.add(description);
            }
            JSONObject basis = content.getJSONObject("basis");
            if (basis == null) {
                basis = new JSONObject();
            }
            String title = document.select(".knw_thorough .hd").text();
            basis.put(title, define);
            content.put("basis", basis);
        } else {
            Elements div = document.select(".summary");
            JSONObject o = new JSONObject();
            o.put("wiki_editor", div.select(".wiki_editor > a").text());
            o.put("text", div.select(".editor_mirror").text());
            List<JSONObject> list = new ArrayList<>();
            o.put("references", list);
            JSONObject text;
            for (Element element : div.select(".references tr")) {
                if (element.select("td").size() == 2) {
                    text = new JSONObject().fluentPut("text", element.text());
                    list.add(text);
                }
            }
            String title = document.select(".knw_thorough .hd").text();
            String category = "";
            switch (categoryId.charAt(0)) {
                case '1':
                    category = "overview";
                    break;
                case '2':
                    category = "basis";
                    break;
                case '3':
                    category = "prevention";
                    break;
                case '4':
                    category = "diagnosis";
                    break;
                case '5':
                    category = "treatment";
                    break;
                case '6':
                    category = "follow_up";
                    break;
            }
            JSONObject jsonObject = content.getJSONObject(category);
            if (jsonObject == null) {
                jsonObject = new JSONObject();
            }
            jsonObject.put(title, o);
            content.put(category, jsonObject);
        }
    }

    private void parseLink(Document document, JSONObject content) {
        Elements rulesList = document.select(".Treatment_rules");
        if (rulesList.size() == 0) {
            return;
        }
        String wiki_editor = rulesList.select(".wiki_editor a").text();
        List<JSONObject> list = new ArrayList<>();
        JSONObject jsonObject = new JSONObject().fluentPut("wiki_editor", wiki_editor)
                .fluentPut("link_list", list);
        JSONObject item = null;
        for (Element li : rulesList.select("li")) {
            Elements a = li.select("> a");
            String title = a.text();
            String url = a.attr("abs:href");
            item = new JSONObject().fluentPut("title", title)
                    .fluentPut("url",url);
            list.add(item);
        }
        List<JSONObject> referencesList = new ArrayList<>();
        jsonObject.put("references", referencesList);
        JSONObject text = null;
        for (Element element : rulesList.select(".references tr")) {
            if (element.select("td").size() == 2) {
                text = new JSONObject().fluentPut("text", element.text());
                list.add(text);
            }
        }
        JSONObject treatment = content.getJSONObject("treatment");
        if (treatment == null) {
            treatment = new JSONObject();
        }
        String title = document.select(".knw_thorough .hd").text();
        treatment.put(title, jsonObject);
        content.put("treatment", treatment);
    }

    private void parseTable(Document document, JSONObject content) {
        Elements diffTable = document.select(".diff_diag");
        if (diffTable.size() == 0) {
            return;
        }
        String wiki_editor = diffTable.select(".wiki_editor a").text();
        List<JSONObject> list = new ArrayList<>();
        JSONObject jsonObject = new JSONObject().fluentPut("wiki_editor", wiki_editor)
                .fluentPut("table", list);
        JSONObject item = null;
        for (Element tr : diffTable.select("tbody > tr")) {
            Element th = tr.select("th").first();
            String name = th.text();
            String symptom = th.nextElementSibling().text();
            String identification = th.nextElementSibling().nextElementSibling().text();
            item = new JSONObject().fluentPut("疾病名", name)
                    .fluentPut("体征/症状鉴别", symptom)
                    .fluentPut("检验鉴别", identification);
            list.add(item);
        }
        JSONObject diagnosis = content.getJSONObject("diagnosis");
        if (diagnosis == null) {
            diagnosis = new JSONObject();
        }
        String title = document.select(".knw_thorough .hd").text();
        diagnosis.put(title, jsonObject);
        content.put("diagnosis", diagnosis);
    }

    private void parseMore(Document document, JSONObject content) {

    }

}
