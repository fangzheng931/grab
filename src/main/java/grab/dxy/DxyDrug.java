package grab.dxy;

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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.RequestBuilder;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.io.ByteArrayInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class DxyDrug {//TODO 验证码+每日访问限制
    private static final Pattern PatternCategory = Pattern.compile("return selectCate\\(this, ?(?<cate>\\d+)\\)");
    private static final Pattern PatternDetailUrl = Pattern.compile("\\Qhttp://drugs.dxy.cn/drug/\\E(?<id>\\d+)\\.htm");
    private static final String database = "jiankangdangan";
    private static final MongoEntityClient mongo = new MongoEntityClient("101.132.96.214", 27017, database, "jiankangdangan_user", "shanzhen@2020");
    private static final MongoEntityTable<MongoJsonEntity> briefTable = mongo.getJsonTable(database, "DxyDrug-Brief");
    private static final MongoEntityTable<MongoBytesEntity> detailTable = mongo.getBytesTable(database, "DxyDrug-Detail");
    private static final MongoEntityTable<MongoBytesEntity> listTable = mongo.getBytesTable(database, "DxyDrug-List");
    private HttpClient client = new HttpClient();

    public static void main(String[] args) {
//        new DxyDrug().grabBrief();
        DxyDrug dxyDrug = new DxyDrug();
        Progress progress = new Progress("detail", briefTable.count(null), 5000);
        for (MongoJsonEntity entity : briefTable.find(null)) {
            dxyDrug.grabDetail(entity.getId());
            progress.increase(1);
        }
    }


    private Document getDocument(String url, String s) {
        Document document = client.tryGet(url, -1, HttpContent::toDocument, d -> !d.select("#kaptchaImage, p:contains(今天的访问次数用完，请明天继续访问！), " + s).isEmpty());
        if (!document.select("p:contains(今天的访问次数用完，请明天继续访问！)").isEmpty()) {
            throw new RuntimeException("今天的访问次数用完，请明天继续访问！");
        }
        if (document.select("#kaptchaImage").isEmpty()) {
            return document;
        }
        Element form = document.select("form[name=validation]").first();
        RequestBuilder post = HttpUtil.post(form.attr("abs:action"));
        for (Element input : form.select("input[type=hidden][name]:not(#validateCode)")) {
            post.addParameter(input.attr("name"), input.attr("value"));
        }
        try {
            HttpContent image = client.get(document.select("#kaptchaImage").attr("abs:src"));
            try (ByteArrayInputStream is = new ByteArrayInputStream(image.getBytes())) {
                //TODO 验证码识别
                String validateCode = JOptionPane.showInputDialog(null, new ImageIcon(ImageIO.read(is)), "Captcha image", JOptionPane.PLAIN_MESSAGE);
                post.addParameter("validateCode", validateCode);
                client.doRequest(post.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return getDocument(url, s);
    }

    private void grabBrief() {
        Document document = getDocument("http://drugs.dxy.cn/index.htm", "[name=cur_cate]");
        for (Element cur_cate : document.select("[name=cur_cate]")) {
            String onclick = cur_cate.select("a").attr("onclick");
            Matcher matcher = PatternCategory.matcher(onclick);
            if (matcher.find()) {
                String group = matcher.group("cate");
                for (Element a : document.select("#cate_" + group + " h3 a")) {
                    String url = a.attr("abs:href");
                    grabCategory(a.text(), url);
                }
            }
        }
    }

    private void grabCategory(String category, String prefix) {

        String maxId = prefix.replace("http://drugs.dxy.cn/category/", "")
                .replace(".htm", "") + "-" + 6;
        if (listTable.exists(maxId)) {
            return;
        }

        for (int page = 1, totalPage = -1; page == 1 || page <= totalPage && page <= 50; page++) {
            String url = prefix + "?page=" + page;
            System.out.println(url);
            Document document = getDocument(url, ":containsOwn(该分类下共有药品：) > span");
            if (totalPage == -1) {
                int count = Integer.parseInt(document.select(":containsOwn(该分类下共有药品：) > span").text());
                totalPage = (count - 1) / 10 + 1;
//				log.info(String.format("%s %d items, %d pages", category, count, totalPage));
            }
            {//TODO save category list page
                String curId = prefix.replace("http://drugs.dxy.cn/category/", "")
                        .replace(".htm", "") + "-" + page;
                String lastId = curId.replace("-" + page, "-" + totalPage);
                //判断是否存在该页面
                if (listTable.exists(lastId)) {
                    return;
                } else if (listTable.exists(curId)) {
                    System.out.println(lastId);
                    while (listTable.exists(curId)) {
                        curId = curId.replace("-" + page++, "-" + page);
                    }
                    page--;
                    continue;
                }
                Elements container = document.select("#container");
                container.select("> :not(.common_hd):not(.common_bd):not(.pagination)").remove();
                listTable.save(new MongoBytesEntity(curId, container.html().getBytes(), new JSONObject().fluentPut("url", url)));
            }
            for (Element li : document.select(".result li")) {
                String detailUrl = li.select("a").attr("abs:href");
                Matcher matcher = PatternDetailUrl.matcher(detailUrl);
                if (!matcher.find()) {
                    throw new RuntimeException(detailUrl);
                }
                String id = matcher.group("id");
                if (briefTable.exists(id)) {
                    briefTable.addToMetadata(id, "category", category);
                    continue;
                }
                JSONObject o = new JSONObject();
                o.put("title", li.select("h3").text());
                o.put("url", detailUrl);
                for (Element b : li.select("p b")) {
                    String key = b.text();
                    StringBuilder value = new StringBuilder();
                    for (Node node = b.nextSibling(); node != null && !(node instanceof Element && ((Element) node).tagName().equals("b")); node = node.nextSibling()) {
                        String line = "";
                        if (node instanceof TextNode) {
                            line = StringUtils.trim(((TextNode) node).getWholeText());
                        } else if (node instanceof Element) {
                            line = StringUtils.trim(((Element) node).text());
                        } else {
                            System.err.println(node);
                        }
                        if (!line.isEmpty()) {
                            value.append(line).append('\n');
                        }
                    }
                    o.put(key, value.toString().replaceAll("\n+", "\n").replaceAll("\n$", ""));
                }
                briefTable.save(new MongoJsonEntity(id, o, new JSONObject().fluentPut("category", new JSONArray().fluentAdd(category))));
            }
        }
    }

    private void grabDetail(String id) {
        if (detailTable.exists(id)) {
            return;
        }
        String url = "http://drugs.dxy.cn/drug/" + id + ".htm";
        Document document = client.tryGet(url, 2, HttpContent::toDocument, p -> !p.select("p").select(".drugname").text().equals("今天的访问次数用完，请明天继续访问！"));
        String html = document.select(".detail").html();
        detailTable.save(new MongoBytesEntity(id, html.getBytes(), new JSONObject().fluentPut("url", url)));
    }
}
