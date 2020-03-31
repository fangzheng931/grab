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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.jws.Oneway;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Haodaifuzhaoyao {
    private static final String database = "jiankangdangan";
    private static final Pattern PatternDetailUrl = Pattern.compile("\\Qhttps://www.haodf.com/yaopin/\\E(?<id>\\d+)\\.htm");
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, "jiankangdangan", "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "Haodaifu.drug-Brief");
    private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "Haodaifu.drug-Detail");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "Haodaifu.drug-Parsed");
    private HttpClient client = new HttpClient();

    public static void main(String[] args) {
        Haodaifuzhaoyao haodaifuzhaoyao = new Haodaifuzhaoyao();
//        haodaifuzhaoyao.brief();
//        for (MongoJsonEntity entity : briefTable.find(null)){
//            haodaifuzhaoyao.grabDetail(entity);
//        }

        Progress progress = new Progress("detail", detailTable.count(null), 5000);
        for (MongoBytesEntity entity : detailTable.find(null)) {
            haodaifuzhaoyao.parseDetail(entity);
            progress.increase(1);
        }
    }

    private void brief(){
        Document document = client.tryGet("https://www.haodf.com/yaopin", -1).toDocument();
        for (Element drg :document.select(".drug-container .drg-con  > a")){
            String name = drg.text();
            System.out.println(name);
            String url = drg.attr("abs:href");
            System.out.println(url);
            grabBrief(name,url);
        }
    }

    private void grabBrief(String name,String url){
        for (int page = 1, totalPage = -1; page == 1 || page <= totalPage; page++){
//            String id = url.replace("https://www.haodf.com/yaopin/","").replace("_1.html","");
            String url1 = url.replace("_1.html","")+"_"+page+".html";
            Document document = client.tryGet(url1, -1).toDocument();
            if (totalPage == -1 && !document.select(".page_turn_a font").isEmpty()){
                totalPage = Integer.parseInt(document.select(".page_turn_a font").text());
            }
            for (Element list : document.select(".drug-list.clearfix .drug-list-con")){
                String detailUrl = list.select(" a").attr("abs:href");
                Matcher matcher = PatternDetailUrl.matcher(detailUrl);
                if (!matcher.find()) {  //find可以对任意位置字符串匹配,其中start为起始查找索引值。
                    throw new RuntimeException(detailUrl);
                }
                String id = matcher.group("id");
                System.out.println(id);
                if (briefTable.exists(id)) {
                    briefTable.addToMetadata(id, "name", name);
                    continue;
                }
                JSONObject json = new JSONObject();
                json.put("title",list.select(" a").text());
                String[] guige = list.select(" span").text().split("：");
                if (guige.length==2){
                    json.put(guige[0],guige[1]);
                }
                for (Element listDetail : list.select(".drug-list-info")){
                    String[] listInfo = listDetail.text().split("：");
                    if (listInfo.length==2){
                        json.put(listInfo[0],listInfo[1]);
                    }
                }
                briefTable.save(new MongoJsonEntity(id,json,new JSONObject().fluentPut("name", new JSONArray().fluentAdd(name))));
            }
        }


    }

    private void grabDetail(MongoJsonEntity entity){
        String id = entity.getId();
        if (detailTable.exists(id)) {
            return;
        }

        String url = "https://www.haodf.com/yaopin/" + id + ".html";
        Document document = client.tryGet(url, -1, HttpContent::toDocument, null);
        Elements elements = document.select(".content.clearfix");
        if (elements.size()!=0){
            detailTable.save(new MongoBytesEntity(id, elements.html().getBytes(), new JSONObject().fluentPut("url", url)));
        }

    }

    private void parseDetail(MongoBytesEntity entity){
//        if (parsedTable.exists(entity.getId())){
//            return;
//        }
        Document document = Jsoup.parse(new String(entity.getBytesContent()));

        JSONObject json = new JSONObject();
        JSONObject basic = new JSONObject();
        String detailUrl = "https:"+document.select(".first-tit a").attr("href");

//        Document document1 = client.tryGet(detailUrl, -1, HttpContent::toDocument, d -> !d.select(".drug-con-info").isEmpty());
        Document document1 = client.tryGet(detailUrl, -1).toDocument();

        for (Element info : document1.select(".drug-con-info")){
            basic.put(info.select("> h2").text(),info.select("> p").text());
        }
        json.put("basic", basic);
        json.put("title",document.select(".first-tit").text());
        for (Element info : document.select(".drug-info-detail")){
            if (!info.ownText().isEmpty()){
                json.put(info.select(".blod").text().replace("：",""),info.ownText());
            }else {
                json.put(info.select(".blod").text().replace("：",""), info.select(".lable").stream()
                        .map(Element::text)
                                .collect(Collectors.toList()));
            }

        }
        for (Element p : document.select(".drug-img p")){
            String[] pDetail = p.text().split("：");
            if (pDetail.length == 2){
                json.put(pDetail[0],pDetail[1]);
            }
        }
        String drugTogether = document.select(".drug-together .f18").text(); //药品搭配
        List<JSONObject> paragraphs = new ArrayList<>();
        json.put(drugTogether, paragraphs);
        JSONObject paragraph = new JSONObject();
        List<String> thText = new ArrayList<>();
        for (Element th : document.select(".together-con th")){
            thText.add(th.text());
        }
        for (Element tr : document.select(".together-con tr")){
            int count =0;
            paragraph = new JSONObject();
            if (tr.select("> td").isEmpty()){
                continue;
            }
            for (Element td : tr.select("> td")){
                paragraph.fluentPut(thText.get(count++),td.text());
            }
            paragraphs.add(paragraph);
        }
        parsedTable.save(new MongoJsonEntity(entity.getId(), json, entity.getJsonMetadata()));
    }
}
