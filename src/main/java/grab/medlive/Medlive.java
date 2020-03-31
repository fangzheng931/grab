package grab.medlive;

import com.alibaba.fastjson.JSONObject;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Medlive {
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "medlive-Brief");
    private static final MongoEntityTable<MongoJsonEntity> detailTable = mongo.getJsonTable(database, "medlive-Detail");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "medlive-Parsed");

    private HttpClient client = new HttpClient();

    private static final HashMap<String, String> map = new HashMap<>();

    public static void main(String[] args) {

//        new Medlive().grabBrief();
        Medlive medlive = new Medlive();
//        Progress progress = new Progress("brief", briefTable.count(null), 5000);
//        for (MongoJsonEntity entity : briefTable.find(null)) {
//            medlive.grabDetail(entity);
//            progress.increase(1);
//        }
        medlive.initMap();
        Progress progress = new Progress("detail", detailTable.count(null), 5000);
        for (MongoJsonEntity entity : detailTable.find(null)) {
            medlive.parseDetail(entity);
            progress.increase(1);
        }

    }

    private void initMap() {
        map.put("简介", "introduction");
        map.put("名称与编码", "nameCoding");
        map.put("定义", "define");
        map.put("病因", "cause");
        map.put("病理解剖", "pathologicalAnatomy");
        map.put("病理生理", "pathophysiology");
        map.put("预防", "prevent");
        map.put("筛检", "screening");
        map.put("问诊与查体", "interrogationAndExamination");
        map.put("辅助检查", "auxiliaryInspection");
        map.put("并发症", "complications");
        map.put("诊断标准", "diagnosticCriteria");
        map.put("治疗目标", "targetOfTreatment");
        map.put("预后", "prognosis");
    }

    private void grabBrief() {
        Document categoryDocument = client.tryGet("http://disease.medlive.cn/wiki/list/171", -1, HttpContent::toDocument, null);
        for (Element li : categoryDocument.select(".sortOutline li")) {
            String category = li.select("> a").text();
            String categoryUrl = li.select("> a").attr("abs:href");
            String categoryId = categoryUrl.replace("http://disease.medlive.cn/wiki/list/", "");
            Document document = client.tryGet(categoryUrl, -1, HttpContent::toDocument, null);
            Elements dd = document.select(".sortDetail dd");
            for (Element a : dd.select("> a")) {
                if (a.text().equals("")) {
                    continue;
                }
                String status = a.select("> span").text();
                String briefUrl = a.attr("abs:href");
                Document briefDocument = client.tryGet(briefUrl, -1, HttpContent::toDocument, null);
                JSONObject o = new JSONObject().fluentPut("status", status.replace("(", "").replace(")", ""));
                //没有 brief 页面时（有：http://disease.medlive.cn/gather/Alport%E7%BB%BC%E5%90%88%E5%BE%81
                // 无：http://disease.medlive.cn/gather/%E8%83%86%E5%9B%8A%E7%99%8C）
                if (briefDocument.select(".wiki_body").isEmpty()) {
                    if (briefDocument.select(".main").isEmpty()) {
                        System.out.println(category + "-" + briefUrl);
                        continue;
                    }
                    String curUrl = briefDocument.select(".current").attr("abs:href");
                    String[] strings = curUrl.split("_")[0].split("/");
                    String id = strings[strings.length - 1];
                    String url = "http://disease.medlive.cn/wiki/essentials_" + id;
                    String name = briefDocument.select(".knw_overview > .hd span").text().replace(" -", "");
                    o.put("url", url);
                    o.put("name", name);
                    briefTable.save(new MongoJsonEntity(id, o));
                    briefTable.addToMetadata(id, "category", new JSONObject().fluentPut("id", categoryId).fluentPut("name", category));
                    continue;
                }
                String url = briefDocument.select(".case_name > a").attr("abs:href");
                String name = briefDocument.select(".case_name > label").text();
                List<JSONObject> list = new ArrayList<>();
                JSONObject caseObject = null;
                for (Element boxTitle : briefDocument.select(".box_title")) {
                    String title = boxTitle.select("> label").text();
                    String text = boxTitle.nextElementSibling().text();
                    caseObject = new JSONObject().fluentPut("title", title)
                            .fluentPut("text", text);
                    list.add(caseObject);
                }
                o.put("name", name);
                o.put("url", url);
                o.put("case", list);
                String id = url.replace("http://disease.medlive.cn/wiki/essentials_", "");
                briefTable.save(new MongoJsonEntity(id, o));
                briefTable.addToMetadata(id, "category", new JSONObject().fluentPut("id", categoryId).fluentPut("name", category));
            }
            System.out.println(categoryId + "-----" + category);
        }
    }

    private void grabDetail(MongoJsonEntity entity) {
        String id = entity.getId();
        String url = "http://disease.medlive.cn/wiki/entry/" + id + "_101_0";
        Document document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".main").isEmpty());
        List<JSONObject> list = new ArrayList<>();
        for (Element chapterList : document.select(".chapter")) {
            String dt = chapterList.select("dt").text();
            Elements elements = chapterList.select("a");
            Elements nodata = elements.select(".nodata");
            elements.removeAll(nodata);
            for (Element a : elements) {
                url = a.attr("abs:href");
                if (!url.contains("http://disease.medlive.cn/wiki/entry/" + id)) {
                    continue;
                }
                String detailId = url.split("_")[1];
                String title = a.text();
                Document detailDocument = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select(".main").isEmpty());
                Elements select = detailDocument.select(".main");
                JSONObject item = new JSONObject().fluentPut("title", title)
                        .fluentPut("id", detailId)
                        .fluentPut("category", dt)
                        .fluentPut("html", select.html().getBytes());
                list.add(item);
            }
        }
        JSONObject json = new JSONObject().fluentPut("name", entity.getJsonContent().get("name")).fluentPut("htmls", list);
        detailTable.save(new MongoJsonEntity(id, json, entity.getJsonMetadata()));
    }

    private void parseDetail(MongoJsonEntity entity) {
        List<JSONObject> htmls = (List<JSONObject>) entity.getJsonContent().get("htmls");
        String name = entity.getJsonContent().getString("name");
        JSONObject json = new JSONObject().fluentPut("name", name);
        for (JSONObject content : htmls) {
            Document document = Jsoup.parse(new String(content.getBytes("html")));
            String title = content.getString("title");
            switch (title) {
                case "名称与编码":
                    parseNameCoding(document, json, title);
                    break;
                case "定义":
                    parseDefine(document, json, title);
                    break;
                case "问诊与查体":
                    parseMore1(document, json, title);
                    break;
                case "辅助检查":
                    parseMore2(document, json, title);
                    break;
                case "并发症":
                case "诊断标准":
                    parseMore3(document, json, title);
                    break;
                case "简介":
                case "病因":
                case "病理解剖":
                case "病理生理":
                case "预防":
                case "筛检":
                case "治疗目标":
                case "预后":
                    parseDefault(document, json, title);
                default:
                    break;
            }
        }
        parsedTable.save(new MongoJsonEntity(entity.getId(), json, entity.getJsonMetadata()));
    }

    private String getContentCategory(String category) {
        return map.get(category);
    }

    private void getContentWikiEditor(Document document, JSONObject json) {
        Elements select = document.select(".wiki_editor");
        if (select.size() == 0) {
            return;
        }
        String wiki_editor = select.select("a").text();
        json.put("wiki_editor", wiki_editor);
    }

    private void getContentReferences(Document document, JSONObject json) {
        Elements elements = document.select(".references");
        if (elements.size() == 0) {
            return;
        }
        List<JSONObject> references = new ArrayList<>();
        JSONObject reference = null;
        for (Element tr : elements.select("tr")) {
            if (tr.select("> td").size() != 2) {
                continue;
            }
            reference = new JSONObject().fluentPut("text", tr.text());
            references.add(reference);
        }
        json.put("references", references);
    }


    private void parseDefault(Document document, JSONObject content, String title) {
        Elements summary = document.select(".summary > .editor_mirror");
        String text = summary.text();
        JSONObject object = new JSONObject().fluentPut("content", text);
        getContentWikiEditor(document, object);
        getContentReferences(document, object);
        content.put(getContentCategory(title), object);
    }

    private void parseDefine(Document document, JSONObject content, String title) {
        List<JSONObject> defines = new ArrayList<>();
        JSONObject object = new JSONObject().fluentPut("defines", defines);
        JSONObject define = null;
        for (Element element : document.select(".define > .descrip")) {
            String text = element.select("> div").text();
            String source = element.select("> p").text().replace(element.select(".bold").text(), "");
            define = new JSONObject().fluentPut("text", text)
                    .fluentPut("source", source);
            defines.add(define);
        }
        getContentWikiEditor(document, object);
        getContentReferences(document, object);
        content.put(getContentCategory(title), object);
    }

    private void parseNameCoding(Document document, JSONObject content, String title) {
        List<JSONObject> nameCoding = new ArrayList<>();
        JSONObject object = new JSONObject().fluentPut("nameCoding", nameCoding);
        for (Element element : document.select(".codingItem")) {
            JSONObject item = new JSONObject();
            for (Element li : element.select("li")) {
                String name = li.select("> span").text().replace("：", "");
                String text = li.select("> p").text();
                item.put(name, text);
            }
            nameCoding.add(item);
        }
        getContentWikiEditor(document, object);
        getContentReferences(document, object);
        content.put(getContentCategory(title), object);
    }

    private void parseMore1(Document document, JSONObject content, String title) {
        JSONObject object = new JSONObject();
        for (Element element : document.select(".dis_t2")) {
            String listName = element.text();
            List<JSONObject> list = new ArrayList<>();
            object.put(listName, list);
            Elements elements = element.nextElementSiblings();
            JSONObject item = new JSONObject();
            for (Element li : elements.select("li")) {
                String name = li.select(".dis_t3").text();
                String text = li.select(".dis_cont").text();
                item.put("title", name);
                item.put("text", text);
                list.add(item);
            }
        }
        getContentWikiEditor(document, object);
        getContentReferences(document, object);
        content.put(getContentCategory(title), object);
    }

    private void parseMore2(Document document, JSONObject content, String title) {
        JSONObject object = new JSONObject();
        for (Element element : document.select(".dis_t2")) {
            String listName = element.text();
            List<JSONObject> list = new ArrayList<>();
            object.put(listName, list);
            Elements elements = element.nextElementSiblings();
            JSONObject item = new JSONObject();
            for (Element li : elements.select("li")) {
                String name = li.select(".dis_t3").text();
                JSONObject table = new JSONObject();
                for (Element tr : li.select(".dis_cont tr")) {
                    table.put(tr.select("th").text(), tr.select("td").text());
                }
                item.put("title", name);
                item.put("text", table);
                list.add(item);
            }
        }
        getContentWikiEditor(document, object);
        getContentReferences(document, object);
        content.put(getContentCategory(title), object);
    }

    private void parseMore3(Document document, JSONObject content, String title) {

        Elements elements = document.select(".dis_panel_list");
        List<JSONObject> list = new ArrayList<>();
        JSONObject object = new JSONObject().fluentPut(title, list);
        JSONObject item = new JSONObject();
        for (Element li : elements.select("> li")) {
            String name = li.select(".dis_t3").text();
            String text = li.select(".dis_cont").text();
            item.put("title", name);
            item.put("text", text);
            list.add(item);
        }
        getContentWikiEditor(document, object);
        getContentReferences(document, object);
        content.put(getContentCategory(title), object);
    }
}



















