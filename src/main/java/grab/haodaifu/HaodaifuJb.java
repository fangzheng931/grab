package grab.haodaifu;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wayue.olympus.common.Progress;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.mongo.MongoBytesEntity;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import org.apache.poi.hssf.record.PageBreakRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HaodaifuJb {
    private static final Pattern PatternDetailUrl = Pattern.compile("\\Qhttps://www.haodf.com/jibing/\\^[a-z]+$\\.htm");
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, "jiankangdangan", "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "Haodaifu-Brief");
    private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "Haodaifu-Detail");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "Haodaifu-Parsed");
    private static final MongoEntityTable<MongoJsonEntity> standardTable = mongo.getJsonTable(database, "Haodaifu-Standard");
    private static final MongoEntityTable<MongoJsonEntity> articlesTable = mongo.getJsonTable(database, "Haodaifu-articles");
    private static final MongoEntityTable<MongoJsonEntity> topicsTable = mongo.getJsonTable(database, "Haodaifu-topics");
    private HttpClient client = new HttpClient();

    public static void main(String[] args) {
        HaodaifuJb haodaifuJb = new HaodaifuJb();
//        haodaifuJb.grabBrief();
//        for (MongoJsonEntity entity : briefTable.find(null)) {
//			haodaifuJb.grabDetail(entity.getId());
//		}

        Progress progress = new Progress("detail", detailTable.count(null), 5000);
        for (MongoBytesEntity entity : detailTable.find(null)) {
//            haodaifuJb.parseDetail(entity);
            haodaifuJb.parseDetailAT(entity);
            progress.increase(1);
        }
    }

    private void parseDetail(MongoBytesEntity entity){
        Document document = Jsoup.parse(new String(entity.getBytesContent()));
        JSONObject json = new JSONObject();
        json.put("title",document.select(".dis_description h1").text());
        json.put("summary",document.select(".hot_recommend p").text());
        List<JSONObject> paragraphs = new ArrayList<>();
        json.put("paragraphs", paragraphs);
        JSONObject paragraph = null;
        for (Element element : document.select(".recommend_main")){
            paragraph = new JSONObject().fluentPut("title",element.select("h2").text())
                    .fluentPut("texts",element.select(".js-longcontent").text());
            paragraphs.add(paragraph);
        }
        parsedTable.save(new MongoJsonEntity(entity.getId(), json, entity.getJsonMetadata()));
    }

    private void parseDetailAT(MongoBytesEntity entity){
        Document document = Jsoup.parse(new String(entity.getBytesContent()));
        JSONObject json1 = new JSONObject();//存科普文章
        JSONObject json2 = new JSONObject();//存专题
        List<JSONObject> articles = new ArrayList<>();
        List<JSONObject> topics = new ArrayList<>();
        json1.put("articles",articles);
        json2.put("topics",topics);
        JSONObject article = null;
        JSONObject topic  = null;
        Document doc = client.tryGet("https://www.haodf.com/jibing/"+entity.getId()+"/jieshao.htm", -1,HttpContent::toDocument,d -> !d.select(".nav-con.pt20.clearfix").isEmpty());
        for (Element li : doc.select(".nav-con.pt20.clearfix  li")){
            if (li.select("> a").text().equals("科普文章")){
                String url = li.select("> a").attr("abs:href");
                Document document1 = client.tryGet(url, -1).toDocument();
                for (int page = 1, totalPage = -1; page == 1 || page <= totalPage; page++){
                    String url1 = "https://www.haodf.com/jibing/"+entity.getId()+"/article_0_"+page+".htm";
                    Document document2 = client.tryGet(url1, -1,HttpContent::toDocument,d -> !d.select(".w880.fl").isEmpty());
                    if (totalPage == -1 && !document2.select(".page_turn font").isEmpty()){
                        totalPage = Integer.parseInt(document2.select(".page_turn font").text());
                    }
                    if (document2.select(".dis_article").isEmpty()){
                        return;
                    }
                    for (Element element : document2.select(".dis_article")){
                        article = new JSONObject().fluentPut("title", element.select("h2").text())
                                .fluentPut("url", element.select("h2 > a").attr("abs:href"));
                        articles.add(article);
                    }
                }
            }else if (li.select("> a").text().equals("专题")){
                String url = li.select("> a").attr("abs:href");
                Document document1 = client.tryGet(url, -1).toDocument();
                for (int page = 1, totalPage = -1; page == 1 || page <= totalPage; page++){
                    String url1 = "https://www.haodf.com/jibing/"+entity.getId()+"/topic_0_"+page+".htm";
                    Document document2 = client.tryGet(url1, -1,HttpContent::toDocument,d -> !d.select(".liney.clearfix").isEmpty());
                    if (totalPage == -1 && !document2.select(".page_turn font").isEmpty() ){
                        totalPage = Integer.parseInt(document2.select(".page_turn font").text());
                    }
                    if (document2.select(".kart_li .fl.kart_title").isEmpty()){
                        return;
                    }
                    for (Element element : document2.select(".kart_li .fl.kart_title")){
                        topic = new JSONObject().fluentPut("title", element.text())
                                .fluentPut("url", element.attr("abs:href"));
                        topics.add(topic);
                    }
//                    json.put("topic", document2.select(".kart_li .fl.kart_title").stream()
//                            .map(a -> new JSONObject()
//                                    .fluentPut("title", a.select("> h2").text())
//                                    .fluentPut("url", a.attr("abs:href")))
//                            .collect(Collectors.toList()));
                }
            }
        }

        articlesTable.save(new MongoJsonEntity(entity.getId(), json1, entity.getJsonMetadata()));
        topicsTable.save(new MongoJsonEntity(entity.getId(), json2, entity.getJsonMetadata()));
    }

    private void grabBrief(){
        Document document = client.tryGet("https://www.haodf.com/jibing/list.htm", -1,HttpContent::toDocument, d -> !d.select(".kstl").isEmpty());
        for (Element kstl : document.select("[class=kstl]")) { //儿科学、妇产科学.....
            String name = kstl.select("> a").text();
            String url = kstl.select("> a").attr("abs:href");
            Document document1 = client.tryGet(url, -1).toDocument();
            List<String> list = new ArrayList<>();
            if (document.select("[class=ksbd]").isEmpty()){
                for (Element li : document1.select(".m_ctt_green li")){
                    if (li.select("> a").text().equals("")){
                        continue;
                    }
                    String url2 = li.select("> a").attr("abs:href");
                    grabBrief(name,url2);
                }
            }
            for (Element ksbd : document1.select(".ksbd > ul > li")){   //儿科、新生儿科....
                String name1 = ksbd.select("> a").text();
                String url1 = ksbd.select("> a").attr("abs:href");
//                list.add(name1);
//                System.out.println(url1);
//                grabBrief(url1);
                Document document2 = client.tryGet(url1, -1,HttpContent::toDocument,d -> !d.select(".ct").isEmpty());
                for (Element li : document2.select(".m_ctt_green li")){
                    if (li.select("> a").text().equals("")){
                        continue;
                    }
                    String url2 = li.select("> a").attr("abs:href");
                    grabBrief(name1,url2);

                }
            }


        }
    }

    private void grabBrief(String name,String url){
//        Matcher matcher = PatternDetailUrl.matcher(url);
//        if (!matcher.find()) {  //find可以对任意位置字符串匹配,其中start为起始查找索引值。
//            throw new RuntimeException(url);
//        }

        String idPrefix = url.replace("https://www.haodf.com/jibing/","");
        String id = idPrefix.replace(".htm","");
        if (briefTable.exists(id)) {
            briefTable.addToMetadata(id, "name", name);
            return;
        }
        Document document = client.tryGet(url, -1,HttpContent::toDocument,d -> !d.select(".d-nav-con .s-cur").isEmpty());
        JSONObject o = new JSONObject();

        String title = document.select(".s-cur").text();
        o.put("title",title);
        Elements element = document.select(".d-dis-title");
        String url3 = element.select("a").attr("abs:href");
        o.put("url",url3);
        String text = document.select(".dis-obj").text().replace("全文阅读>","");
        o.put("text",text);
        briefTable.save(new MongoJsonEntity(id, o, new JSONObject().fluentPut("name", new JSONArray().fluentAdd(name))));
    }

    private void grabDetail(String id){
        if (detailTable.exists(id)) {
            return;
        }
        String url = "https://www.haodf.com/jibing/"+id+"/jieshao.htm";
        Document document = client.tryGet(url, -1, HttpContent::toDocument, null);
        Elements elements = document.select(".content.pt20");
        elements.select(".w320 fr").remove();
        detailTable.save(new MongoBytesEntity(id, elements.html().getBytes(), new JSONObject().fluentPut("url", url)));
    }
}
