package com.tachyon.news.template.config;

import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.NewsConstants;
import com.tachyon.news.template.model.Keyword;
import com.tachyon.news.template.model.User;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
@Slf4j
@Component
public class MyContext {
    @Value("${kongsi.json.targetPath}")
    private String jsonTargetPath;
    @Value("${kongsi.html.targetPath}")
    private String htmlTargetPath;
    @Value("${kongsi.template.queues}")
    private String templaeQueues;
    @Value("${kongsi.elastic-index:tachyon-news}")
    private String elasticIndex;

    @Value("${kongsi.elastic-type:kongsi}")
    private String elasticType;
    @Value("${kongsi.elastic-port:9000}")
    private int elasticPort;
    @Value("${kongsi.elastic-url:localhost}")
    private String elasticUrl;

    @Value("${kongsi.elastic-bulk-action:1000}")
    private int bulkActionCount;
    @Value("${kongsi.elastic-flush-interval:60}")
    private long flushInterval;
    @Value("${kongsi.elastic-bulk-size:20}")
    private long bulkSize;

    @Value("${kongsi.telegram.chatId}")
    private String chatId;

    private String[] templates;
    private AtomicLong gatewayCount = new AtomicLong();

    private Map<String, String> isuCdNmMap = new HashMap<>();
    @Autowired
    private TemplateMapper templateMapper;

    private Map<String, User> telegramMap = new LinkedHashMap<>();


    public void addGatewayCalling() {
        gatewayCount.incrementAndGet();
    }

    public long getGatewayCalling() {
        return gatewayCount.get();
    }


    @PostConstruct
    public void init() {
        setupTemplates();
        setupTelegramMap();
    }

    public void setupTelegramMap() {
        List<Map<String,Object>> maps = templateMapper.findKeywords();

        List<User> users = covert(maps);
        synchronized (telegramMap) {
            telegramMap.clear();
            for (User user : users) {
                telegramMap.put(user.getUserid(), user);
            }
        }
        for (String key : telegramMap.keySet()) {
            log.info("<<< "+key + " " + telegramMap.get(key));
        }
    }

    public Map<String, User> getTelegramMap() {
        return telegramMap;
    }

    private List<User> covert(List<Map<String, Object>> maps) {
        Map<String, List<Map<String, Object>>> map = new HashMap<>();
        for (Map<String, Object> _map : maps) {
            String userId = Maps.getValue(_map,"userid");
            if (map.containsKey(userId)) {
                List<Map<String, Object>> mapList = map.get(userId);
                mapList.add(_map);
            } else {
                List<Map<String, Object>> mapList = new ArrayList<>();
                mapList.add(_map);
                map.put(userId, mapList);
            }
        }

        List<User> users = new ArrayList<>();
        for (String userId : map.keySet()) {
            List<Map<String, Object>> mapList = map.get(userId);
            log.info(userId+" ");
            int chatId = findChatId(mapList.get(0));
            if (chatId == 0) {
                log.info("SkIP "+userId);
                continue;
            }
            users.add(convertToUser(mapList, userId));
        }

        Collections.sort(users, new Comparator<User>() {
            @Override
            public int compare(User o1, User o2) {
                return Integer.valueOf(o1.getGrade()).compareTo(Integer.valueOf(o2.getGrade()));
            }
        });
        return users;
    }

    private User convertToUser(List<Map<String, Object>> maps,String userId) {
        User user = new User();
        user.setUserid(userId);
        user.setGrade(findGrade(maps.get(0)));
        user.setChatId(findChatId(maps.get(0)));

        List<Keyword> keywords = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            String keyword = Maps.getValue(map, "keyword");
            int order = Maps.getIntValue(map, "_order");

            keywords.add(new Keyword(keyword, order));
        }

        Collections.sort(keywords, new Comparator<Keyword>() {
            @Override
            public int compare(Keyword o1, Keyword o2) {
                return Integer.valueOf(o1.getOrder()).compareTo(Integer.valueOf(o2.getOrder()));
            }
        });
        user.setKeywords(keywords);

        return user;
    }

    private int findChatId(Map<String, Object> map) {
        return Maps.getIntValue(map, "chat_id");
    }

    private int findGrade(Map<String, Object> map) {

        return Maps.getIntValue(map, "grade");
    }
    private void setupTemplates() {
        if (isEmpty(templaeQueues)) {
            templates = new String[0];
        } else {
            templates = StringUtils.split(templaeQueues, ",");
        }
    }

    public String getChatId() {
        return chatId;
    }

    public void setChatId(String chatId) {
        this.chatId = chatId;
    }

    public String findComName(String code) {
        if (isuCdNmMap.containsKey(code)) {
            return isuCdNmMap.get(code);
        } else {
            return "";
        }

    }

    public void putCompany(String code, String name) {
        isuCdNmMap.put(code, name);
    }
    public int getBulkActionCount() {
        return bulkActionCount;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public long getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(long bulkSize) {
        this.bulkSize = bulkSize;
    }

    private boolean isEmpty(String value) {
        if (value == null || "" .equalsIgnoreCase(value)) {
            return true;
        } else {
            return false;
        }
    }
    public String findFromRabbitMq(String queueName) {
//        return "rabbitmq:" + queueName;
        return "rabbitmq:" + queueName + "?queue=" + queueName + "&routingKey=" + queueName+"&autoDelete=false&declare=true";
    }


    public String getJsonTargetPath() {
        return jsonTargetPath;
    }

    public String getHtmlTargetPath() {
        return htmlTargetPath;
    }

    public String getTemplaeQueues() {
        return templaeQueues;
    }

    public String getElasticIndex() {
        return elasticIndex;
    }

    public String getElasticType() {
        return elasticType;
    }

    public int getElasticPort() {
        return elasticPort;
    }

    public String getElasticUrl() {
        return elasticUrl;
    }

    public String[] getTemplates() {
        return templates;
    }

    public String findInputValue(Message message) {
        byte[] bytes = message.getBody();
        return new String(bytes, Charset.forName("UTF-8"));
    }
    private String getHeader(Message message,String key) {
        return (String) message.getMessageProperties().getHeaders().get(key);
    }
    public String findRoutingKey(Message message) {
        return message.getMessageProperties().getReceivedRoutingKey();
    }

//    public NewsBean findNewsBean(Exchange exchange) {
//        return (NewsBean)exchange.getIn().getHeader("NEWS_BEAN");
//    }
    public String findKongsiMapKey(Message message) {
        return getHeader(message,NewsConstants.KONGSI_KEY);
    }

    public Map<String, Object> findKongsiMap(Message message) {
        String key = findKongsiMapKey(message);
        if (isEmpty(key)) {

            return null;
        } else {
            Map<String, Object> kongsi = new HashMap<>();
            String[] strings = StringUtils.split(key, ",");
            for (String _key : strings) {
                kongsi.put(_key, getHeader(message,_key));
            }

            return kongsi;
        }
    }

//    public boolean findMetaKongsi(Exchange exchange) {
//        return (boolean)exchange.getIn().getHeader("META_KONGSI");
//    }
//
//    public boolean findKongsiFile(Exchange exchange) {
//        return (boolean)exchange.getIn().getHeader("KONGSI_FILE");
//    }
}
