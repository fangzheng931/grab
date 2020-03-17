package grab.dxy;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class DxyColumnsSpecial extends DxyColumns {
	private static final Pattern PatternArticleUrl = Pattern.compile("\\Qhttps://dxy.com/column/\\E(?<id>\\d+)");
	private static final String database = "jiankangdangan";
	private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
	public static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "DxyColumnsSpecial-Brief");
	private HttpClient client = new HttpClient();

	public static void main(String[] args) {
		new DxyColumnsSpecial().grabBrief();
	}

	@Override
	protected void grabBrief() {
		String prefix = "https://dxy.com/view/i/columns/special/list?";
		for (int page = 1, totalPage = -1; page == 1 || page <= totalPage; page++) {
			String url = prefix + "&items_per_page=1000&page_index=" + page;
			log.info(url);
			JSONObject json = client.tryGet(url, -1).toJSONObject();
			if (totalPage == -1) {
				totalPage = (int) JSONPath.eval(json, "data.total_pages");
			}
			for (JSONObject object : ((JSONArray) JSONPath.eval(json, "data.items")).toJavaList(JSONObject.class)) {
				grabColumn(object.getString("id"));
			}
		}
	}

	private void grabColumn(String columnId) {
		String columnUrl = "https://dxy.com/column/special/" + columnId;
		log.info(columnUrl);
		Document document = client.tryGet(columnUrl, -1, HttpContent::toDocument, d -> !d.select(".col-article-list").isEmpty());
		String columnTitle = document.select(".col-special-title").text();
		for (Element li : document.select(".col-article-list > ul > li")) {
			String article_url = li.select(".hd-tit .hd a").attr("abs:href");
			Matcher matcher = PatternArticleUrl.matcher(article_url);
			if (!matcher.find()) {
				throw new RuntimeException("invalid article url: " + article_url);
			}
			String id = matcher.group("id");
			JSONObject json = new JSONObject()
					.fluentPut("url", article_url)
					.fluentPut("title", li.select(".hd-tit .hd a").text())
					.fluentPut("author_url", li.select(".author a").attr("abs:href"))
					.fluentPut("author", li.select(".author a").text())
					.fluentPut("description", li.select(".description").text());
			JSONObject meta = new JSONObject()
					.fluentPut("column_title", columnTitle)
					.fluentPut("column_url", columnUrl);
			briefTable.save(new MongoJsonEntity(id, json));
			briefTable.addToMetadata(id, "column", meta);
		}
	}
}
