import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wayue.olympus.common.http.HttpClient;
import com.wayue.olympus.common.http.HttpContent;
import com.wayue.olympus.common.mongo.MongoEntityClient;
import com.wayue.olympus.common.mongo.MongoEntityTable;
import com.wayue.olympus.common.mongo.MongoJsonEntity;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Jiankang163 {
	private static final String database = "jiankangdangan";
	private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
	private static final MongoEntityTable<MongoJsonEntity> detailTable = mongo.getJsonTable(database, "Jiankang163.disease-Parsed");
	private static final MongoEntityTable<MongoJsonEntity> articleTable = mongo.getJsonTable(database, "Jiankang163.articles-Standard");
	private HttpClient client = new HttpClient();

	public static void main(String[] args) {
		new Jiankang163().work();
	}

	private void work() {
//		JSONObject json = client.tryGet("http://shiyong.jiankang.163.com/user/disease/api_k_list.html", 1).toJSONObject();
//		for (JSONObject data : json.getJSONArray("data").toJavaList(JSONObject.class)) {
//		}
//		System.out.println(json);
		for (int page = 1; page <= 9; page++) {
			JSONObject jsonObject = client.tryGet("http://shiyong.jiankang.163.com/user/disease/list.html?k=&size=100&page=" + page, -1, HttpContent::toJSONObject, j -> j.getJSONObject("data").getInteger("pageCount") == 9);
			List<JSONObject> list = jsonObject.getJSONObject("data").getJSONArray("list").toJavaList(JSONObject.class);
			for (JSONObject json : list) {
				String id = json.getString("id");
//				if (detailTable.exists(id)) {
//					continue;
//				}
				String url = json.getString("url");
				Document document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select("#main_text").isEmpty());
				json.put("url", url);
				for (Element script : document.select("script")) {
					String html = script.html();
					if (html.contains("nTabyHtml")) {
						String texts = html.replaceAll("(?s).*nTabyHtml=\\[(?=\n)", "[").replaceAll("(?s)(?<=\n)];.*", "]");
						List<String> text;
						try {
							text = JSON.parseArray(texts).toJavaList(String.class);
						} catch (Exception e) {
							text = new ArrayList<>();
							for (String s : texts.split(",?\n")) {
								if (s.startsWith("'") && s.endsWith("'")) {
									text.add(s.substring(1, s.length() - 1));
								} else if (!s.matches("[\\[\\]]")) {
									throw new RuntimeException(html);
								}
							}
						}
						json.put("texts", text);
					}
				}
				JSONArray articles = new JSONArray();
				json.put("articles", articles);
				grabArticles(document, articles);
				detailTable.save(new MongoJsonEntity(id, json));
			}
		}
	}

	private void grabArticles(Document document, JSONArray articles) {
		for (Element a : document.select(".kepu_box a")) {
			String title = a.select("h3").text();
			String href = a.attr("abs:href");
			articles.add(new JSONObject().fluentPut("title", title).fluentPut("url", href));
			Document content = client.tryGet(href, 1).toDocument();
			if (content.select(".tip:contains(网页跑丢了)").isEmpty()) {
				String article_id = href.replaceAll(".*/|\\.html", "");
				if (articleTable.exists(article_id)) {
					continue;
				}
				JSONObject json = new JSONObject();
				if (!content.select(".post_content_main").isEmpty()) {
					json.put("title", content.select(".post_content_main h1").text());
					json.put("content", content.select(".post_text p").stream()
							.map(Element::text)
							.filter(StringUtils::isNotBlank)
							.collect(Collectors.joining("\n")));
					json.put("time", content.select(".post_time_source").text().replaceAll("[　\\s]*来源.*", ""));
				} else if (!content.select(".content_core").isEmpty()) {
					json.put("author", content.select(".expert_card h3").text());
					json.put("title", content.select(".content_core .f_center").text());
					json.put("content", content.select(".content_core p:not(.f_center)")
							.stream()
							.map(Element::text)
							.filter(StringUtils::isNotBlank)
							.collect(Collectors.joining("\n")));
				} else if (!content.select(".article_wrap").isEmpty()) {
					json.put("time", content.select(".time *:eq(0)").text());
					json.put("author", content.select(".time *:eq(2)").text());
					json.put("title", content.select(".content_core .f_center").text());
					Elements remove = content.select(".content p:contains(精选继续看), .content p:contains(精选继续看) ~ *");
					remove.remove();
					json.put("content", content.select(".content p").stream()
							.map(Element::text)
							.filter(StringUtils::isNotBlank)
							.collect(Collectors.joining("\n")));
				}
				json.put("url", href);
				articleTable.save(new MongoJsonEntity(article_id, json));
			}
		}
	}
}
