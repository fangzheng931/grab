package grab.baidu;

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
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.RequestBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class BaiduBaike {
	private static final String database = "jiankangdangan";
	private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
	private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "BaiduBaike-Brief");
	private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "BaiduBaike-Detail");
	private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "BaiduBaike-Parsed");
	private static final MongoEntityTable<MongoJsonEntity> standardTable = mongo.getJsonTable(database, "BaiduBaike-Standard");
	private HttpClient client = new HttpClient();

	public static void main(String[] args) {
		log.info("start");
		BaiduBaike baiduBaike = new BaiduBaike();
//		baiduBaike.grabBrief();
//		baiduBaike.grabBrief("健康医疗", "76625");
		Progress progress = new Progress("detail", detailTable.count(null), 5000);
		for (MongoBytesEntity entity : detailTable.find(null)) {
			baiduBaike.parseDetail(entity);
			progress.increase(1);
		}
	}

    private void parseDetail(MongoBytesEntity entity) {
        Document document = Jsoup.parse(new String(entity.getBytesContent()));
        JSONObject json = new JSONObject();
        JSONObject basic = new JSONObject();
        for (Element dt : document.select(".basic-info .basicInfo-item.name")) {
            basic.put(dt.text(), dt.nextElementSibling().text());
        }
        json.put("basic", basic);
        json.put("title", document.select(".lemmaWgt-lemmaTitle-title h1").text());
        json.put("summary", document.select(".lemma-summary").text());
        List<JSONObject> paragraphs = new ArrayList<>();
        json.put("paragraphs", paragraphs);
        JSONObject paragraph = null;
        for (Element element : document.select(".anchor-list ~ *")) {
            if (element.hasClass("para-title")) {
                paragraph = new JSONObject()
                        .fluentPut("title", element.select(".title-text").first().ownText())
                        .fluentPut("texts", new JSONArray());
                paragraphs.add(paragraph);
            } else if (element.hasClass("para")) {
                assert paragraph != null;
                paragraph.getJSONArray("texts").add(element.text());
            }
        }
        json.put("tags", document.select("#open-tag-item .taglist").stream()
                .map(Element::text)
                .map(String::trim).
                        collect(Collectors.toList()));
        json.put("articles", document.select(".article-medical-list .atc_link").stream()
                .map(a -> new JSONObject()
                        .fluentPut("title", a.text())
                        .fluentPut("url", a.attr("abs:href")))
                .collect(Collectors.toList()));
        json.put("videos", document.select(".mvm-video-container .video-wrapper").stream()
                .map(div -> new JSONObject()
                        .fluentPut("title", div.select(".video-title").text())
                        .fluentPut("url", div.select("a").attr("abs:href")))
                .collect(Collectors.toList()));

        json = (JSONObject) JsonUtil.escapeBson(json);
        parsedTable.save(new MongoJsonEntity(entity.getId(), json, entity.getJsonMetadata()));
    }

	private void grabDetail(MongoJsonEntity entity) {
		String id = entity.getId();
		String url = entity.getJsonContent().getString("lemmaUrl");
//		log.info("grab detail: " + url);
		Document document = client.tryGet(url, 2, HttpContent::toDocument, d -> !d.select(".main-content").isEmpty());
		if (document == null) {
			return;
		}
		Elements elements = document.select(".poster-left:has(.expert-icon), .main-content");
		elements.select(".top-tool, .tashuo-bottom").remove();
		detailTable.save(new MongoBytesEntity(id, elements.html().getBytes(), new JSONObject().fluentPut("url", url)));
	}

	private void grabBrief() {
		Document document = client.tryGet("https://baike.baidu.com/science/medical", -1).toDocument();
		for (Element li : document.select(".knowledge-list > li")) {
			String name = li.select("> span").text();
			String url = li.select("> a").attr("abs:href");
			String tagId = url.replace("http://baike.baidu.com/wikitag/taglist?tagId=", "");
			if (!tagId.matches("\\d+")) {
				log.error("{} wrong format url:{}", name, url);
				continue;
			}
			grabBrief(name, tagId);
		}
	}

	private void grabBrief(String name, String tagId) {
		Progress progress = null;
		for (int page = 0, totalPage = 1; page == 0 || page < totalPage; page++) {
			RequestBuilder builder = HttpUtil.post("https://baike.baidu.com/wikitag/api/getlemmas")
					.addParameter("limit", "24")
					.addParameter("timeout", "3000")
					.addParameter("filterTags", "[]")
					.addParameter("tagId", tagId)
					.addParameter("fromLemma", "false")
					.addParameter("contentLength", "40")
					.addParameter("page", String.valueOf(page));
			JSONObject json = client.tryRequest(tagId + "." + page, builder, 4, HttpContent::toJSONObject, j -> j.containsKey("lemmaList"));
			if (page == 0) {
				totalPage = json.getInteger("totalPage");
				progress = new Progress(name, totalPage, 1000);
			}
			progress.setProgress(page);
			for (JSONObject lemma : json.getJSONArray("lemmaList").toJavaList(JSONObject.class)) {
				briefTable.save(new MongoJsonEntity(lemma.getString("lemmaId"), lemma));
				briefTable.addToMetadata(lemma.getString("lemmaId"), "tag", new JSONObject().fluentPut("name", name).fluentPut("id", tagId));
			}
		}
	}
}
