package grab.dxy;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.wayue.olympus.common.Progress;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.mongo.MongoBytesEntity;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.stream.Collectors;

@Slf4j
public class DxyColumns {//TODO 只能看到最近的1500条
	private static final String database = "jiankangdangan";
	private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
	private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "DxyColumns-Brief");
	private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "DxyColumns-Detail");
	private static final MongoEntityTable<MongoJsonEntity> parsedTable = mongo.getJsonTable(database, "DxyColumns-Parsed");
	private HttpClient client = new HttpClient();

	public static void main(String[] args) {
		DxyColumns dxyColumns = new DxyColumns();
//		dxyColumns.grabBrief();
//		for (MongoJsonEntity entity : briefTable.find(null)) {
//			dxyColumns.grabDetail(entity.getId());
//		}
//		for (MongoJsonEntity entity : DxyColumnsSpecial.briefTable.find(null)) {
//			dxyColumns.grabDetail(entity.getId());
//		}
		Progress progress = new Progress("detail", detailTable.count(null), 5000);
		for (MongoBytesEntity entity : detailTable.find(null)) {
			dxyColumns.parseDetail(entity);
			progress.increase(1);
		}
	}

	protected void grabBrief() {
		String prefix = "https://dxy.com/view/i/columns/article/list?";
		for (int page = 1, totalPage = -1; page == 1 || page <= totalPage; page++) {
			String url = prefix + "&items_per_page=30&page_index=" + page;
			log.info(url);
			JSONObject json = client.tryGet(url, -1).toJSONObject();
			if (totalPage == -1) {
				totalPage = (int) JSONPath.eval(json, "data.total_pages");
			}
			for (JSONObject object : ((JSONArray) JSONPath.eval(json, "data.items")).toJavaList(JSONObject.class)) {
				String id = object.getString("id");
				briefTable.save(new MongoJsonEntity(id, object));
			}
		}
	}

	protected void grabDetail(String id) {
		if (detailTable.exists(id)) {
			log.info("skip detail: " + id);
			return;
		}
		try {
			Thread.sleep((long) (500 + 500 * Math.random()));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		String url = "https://dxy.com/column/" + id;
//		log.info("grab detail: " + id);
		Document document = client.tryGet(url, 2, HttpContent::toDocument, d -> !d.select(".pg-article-inner").isEmpty());
		if (document == null) {
			return;
		}
		Element article = document.select(".pg-article-inner").first();
		article.select(".pg-article-cnt > :not(.editor-style)").remove();
		article.select("p:has([style=color: rgb(153, 153, 153);])").remove();
		detailTable.save(new MongoBytesEntity(id, article.html().getBytes(), new JSONObject().fluentPut("url", url)));
	}

	private void parseDetail(MongoBytesEntity entity) {
		Document document = Jsoup.parse(new String(entity.getBytesContent()));
		JSONObject json = new JSONObject();
		json.put("title", document.select("h1").text());
		json.put("author", document.select(".author a").text());
		json.put("time", document.select(".time").text());
		Elements p = document.select(".editor-body p");
		p.select("img").remove();
		p.select(":containsOwn(图片来源)").remove();
		json.put("text", p.stream().map(Element::text)
				.filter(s -> !StringUtils.isEmpty(s))
				.collect(Collectors.joining("\n")));
		parsedTable.save(new MongoJsonEntity(entity.getId(), json, entity.getJsonMetadata()));
	}
}
