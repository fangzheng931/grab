package grab.baikemingyi;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

public class Baikemingyi {
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, "jiankangdangan", "jiankangdangan_user", "shanzhen@2020");
    private static MongoCollection<org.bson.Document> collection = mongo.getMongo().getDatabase(database).getCollection("baikemingyi");
    private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "baikemingyi-parsed");

    public static void main(String[] args) {
        Baikemingyi baikemingyi = new Baikemingyi();
        for (org.bson.Document entity : collection.find()){
            baikemingyi.parsedDetail(entity);
        }
    }

    private void parsedDetail(org.bson.Document entity){
       Document document =  Jsoup.parse((String )entity.get("html"));
        JSONObject json = new JSONObject();
        JSONObject basic = new JSONObject();
        json.put("title",document.select(".detail_name").text());
        for (Element element : document.select(".name_info p")){
            String[] s= element.text().split("：");
            if (s.length == 2){
                basic.put(s[0],s[1]);
            }
        }
        json.put("basic",basic);
        List<JSONObject> paragraphs = new ArrayList<>();
        json.put("paragraphs", paragraphs);
        JSONObject paragraph = null;
        for (Element div : document.select(".content > div")){
            String title = div.select("h1").text();
            String texts = div.select("p").text();
            paragraph = new JSONObject()
                    .fluentPut("title", title)
                    .fluentPut("texts", texts);
            paragraphs.add(paragraph);
        }
        parsedTable.save(new MongoJsonEntity((String) entity.get("id"), json, new JSONObject().fluentPut("url",entity.get("url"))));
    }
}
