package grab.dxy;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.mongo.MongoBytesEntity;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class DxyDisease {//TODO https://dxy.com/robots.txt, 采100条就挂
	private static final Pattern PatternGroup = Pattern.compile("\\Qhttps://dxy.com/diseases/\\E(?<group>\\w+)/?");
	private static final String database = "jiankangdangan";
	private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
	private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "DxyDisease-Brief");
	private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "DxyDisease-Detail");
	private HttpClient client = new HttpClient();

	public static void main(String[] args) {
		DxyDisease dxyDisease = new DxyDisease();
//		dxyDisease.grabBrief();
		for (MongoJsonEntity entity : briefTable.find(null)) {
			dxyDisease.grabDetail(entity.getId());
		}
	}

	private void grabBrief() {
		String cssQuery = ".section-navbar-title:containsOwn(科室) + .section-navbar-items li:gt(0)";
		Document document = client.tryGet("https://dxy.com/diseases/", -1, HttpContent::toDocument, d -> !d.select(cssQuery).isEmpty());
		for (Element li : document.select(cssQuery)) {
			String href = li.select("a").attr("href");
			Matcher matcher = PatternGroup.matcher(href);
			if (!matcher.find()) {
				throw new RuntimeException(href);
			}
			String name = li.text(), group = matcher.group("group");
			for (int page = 1, total_pages = 0; page == 1 || page < total_pages; page++) {
				String url = "https://dxy.com/view/i/disease/list?section_group_name=" + group + "&page_index=" + page;
				JSONObject json = client.tryGet(url, -1, c -> JSON.parseObject(c.toString()), j -> JSONPath.eval(j, "data.items") != null);
				JSONArray items = (JSONArray) JSONPath.eval(json, "data.items");
				for (JSONObject item : items.toJavaList(JSONObject.class)) {
					String id = item.getString("id");
					briefTable.save(new MongoJsonEntity(id, item));
				}
				total_pages = (int) JSONPath.eval(json, "data.total_pages");
				log.info(name + "\t" + group + "\t" + total_pages);
			}
		}
	}

	private void grabDetail(String id) {
		if (detailTable.exists(id)) {
			log.info("skip detail: " + id);
			return;
		}
		log.info("grab detail: " + id);
		String url = "https://dxy.com/disease/" + id;
		String cssQuery = ".disease-list";
		Document document = client.tryGet(url, 2, HttpContent::toDocument, d -> !d.select(cssQuery).isEmpty());
		String html = document.select(cssQuery).html();
		detailTable.save(new MongoBytesEntity(id, html.getBytes(), new JSONObject().fluentPut("url", url)));
	}
}
