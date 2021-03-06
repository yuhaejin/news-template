package com.tachyon.news.template.config;

import com.tachyon.crawl.kind.KrxCrawler;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.NewsConstants;
import com.tachyon.news.template.model.Bot;
import com.tachyon.news.template.model.Keyword;
import com.tachyon.news.template.model.User;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
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

    @Value("${kongsi.spclp.path}")
    private String spcLpPath;

    @Value("${kongsi.spclp.keyword}")
    private String spcLpKeyword;
    @Value("${kongsi.skip.key.path}")
    private String skipDocNoIsuCdAcptNoFilePath;

    @Value("${max.pool.size}")
    private int maxPoolSize;
    @Value("${core.pool.size}")
    private int corePoolSize;
    @Value("${queue.capacity}")
    private int queueCapacity;
    @Value("${dir.to.monitor}")
    private String dirToMonitor;

    @Value("${retry.max.attempts}")
    private int retryMaxAttempts;
    @Value("${retry.back.off.period}")
    private long retryBackOffPeriod;

    private String[] templates;
    private AtomicLong gatewayCount = new AtomicLong();

    @Autowired
    private Environment environment;



//    private Map<String, String> INVESTER_NAME = new HashMap<>();


    public long getRetryBackOffPeriod() {
        return retryBackOffPeriod;
    }

    public String getDirToMonitor() {
        return dirToMonitor;
    }

    public void setDirToMonitor(String dirToMonitor) {
        this.dirToMonitor = dirToMonitor;
    }
    private Map<String, String> isuCdNmMap = new HashMap<>();
    @Autowired
    private TemplateMapper templateMapper;
    /**
     * ???????????? ??????.
     * KEY ??? ??????????????? VALUE ?????????????????? ?????????.
     */
    private Map<String, String> INVESTOR_MAP = new HashMap<>();


    private List<String> telegramKeywordList = new ArrayList<>();

    private Map<String, User> telegramMap = new LinkedHashMap<>();

    private Map<String, Integer> QUEUE_MAP = new HashMap<>();

    public void addGatewayCalling() {
        gatewayCount.incrementAndGet();
    }

    public long getGatewayCalling() {
        return gatewayCount.get();
    }


    private Map<String, String> KOSPI200 = new HashMap<>();

    @PostConstruct
    public void init() {
        setupTemplates();
        refreshTelegramKeywordList();
        refreshTelegramUserInfo();
        setupRepresentativeName();
//        viewValue();
//        setupInvestorName();
        refreshKospi200();
    }

    /**
     * ?????????200 ????????? ??????...
     */
    public void refreshKospi200() {

        Map<String,String> temp = new KrxCrawler().findKospi200();
        if (temp.size() != 0) {
            synchronized (KOSPI200) {
                KOSPI200.clear();
                KOSPI200.putAll(temp);
                log.info("?????????200 ?????? "+KOSPI200.size());
            }
        }
    }

    //    private void setupInvestorName() {
//        File dir = new File(getDirToMonitor());
//        if (dir.exists()==false) {
//            return;
//        }
//
//        File[] files = dir.listFiles();
//        for (File f : files) {
//            String name = FilenameUtils.getName(f.getAbsolutePath());
//            if (name.equalsIgnoreCase("investor.txt")) {
//                try {
//                    List<String> lines = FileUtils.readLines(f, "UTF-8");
//                    initializeInvestor(lines);
//                } catch (IOException e) {
//
//                }
//            }
//        }
//    }
    private String findCurrentProfiles() {
        String[] strings = environment.getActiveProfiles();
        if (strings.length == 0) {
            return "prod";
        }
        return strings[0];
    }


    public boolean isDev() {
        if ("dev".equalsIgnoreCase(findCurrentProfiles())) {
            return true;
        } else {
            return false;
        }
    }
    private void viewValue() {
        log.info("spcLpPath="+spcLpPath);
        log.info("spcLpKeyword="+spcLpKeyword);
    }

    public void setupRepresentativeName() {
//        INVESTOR_MAP.put("??????????????????", "????????????");
//        INVESTOR_MAP.put("????????????????????????", "????????????");
//        INVESTOR_MAP.put("??????????????????", "????????????");

        List<Map<String,Object>> maps = templateMapper.findRepresentativeName();
        synchronized (INVESTOR_MAP) {
            INVESTOR_MAP.clear();
            for (Map<String, Object> map : maps) {
                String name = Maps.getValue(map, "name");
                String repNm = Maps.getValue(map, "rep_nm");
                log.info("INVESTOR_MAP "+name+" => "+repNm);
                INVESTOR_MAP.put(name, repNm);
            }
        }
    }

//    public void initializeInvestor(List<String> lines) {
//        if (lines == null || lines.size() == 0) {
//            return;
//        }
//
//        synchronized (INVESTER_NAME) {
//            INVESTER_NAME.clear();
//            for (String line : lines) {
//                if (line.startsWith("#")) {
//                    continue;
//                }
//                log.info("... "+line);
//                String[] strings = StringUtils.splitByWholeSeparator(line, "=");
//                if (strings.length == 2) {
//                    log.info(strings[0]+" >>> "+strings[1]);
//                    INVESTER_NAME.put(strings[0], strings[1]);
//                }
//            }
//        }
//    }

//    public String findInvestorName(String name) {
//        synchronized (INVESTER_NAME) {
//            if (INVESTER_NAME.containsKey(name)==false) {
//                return null;
//            } else {
//                return INVESTER_NAME.get(name);
//            }
//        }
//    }
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public boolean hasRepresentativeName(String investor){
        return INVESTOR_MAP.containsKey(investor);
    }

    public String getSpcLpKeyword() {
        return spcLpKeyword;
    }

    public void setSpcLpKeyword(String spcLpKeyword) {
        this.spcLpKeyword = spcLpKeyword;
    }

    public String findRepresentativeName(String investor) {
        return INVESTOR_MAP.get(investor);
    }

    public String getSkipDocNoIsuCdAcptNoFilePath() {
        return skipDocNoIsuCdAcptNoFilePath;
    }

    public void setSkipDocNoIsuCdAcptNoFilePath(String skipDocNoIsuCdAcptNoFilePath) {
        this.skipDocNoIsuCdAcptNoFilePath = skipDocNoIsuCdAcptNoFilePath;
    }

    public String getSpcLpPath() {
        return spcLpPath;
    }

    public void setSpcLpPath(String spcLpPath) {
        this.spcLpPath = spcLpPath;
    }

    public void refreshTelegramKeywordList() {
        List<Map<String,Object>> maps = templateMapper.findKeywords();

        synchronized (telegramKeywordList) {
            telegramKeywordList.clear();
            for (Map<String,Object> map : maps) {
                String keyword = Maps.getValue(map,"keyword");
                telegramKeywordList.add(keyword);
            }
        }
        for (String key : telegramKeywordList) {
            log.info("??????????????? <<< "+key);
        }
    }
    public Map<String, User> getTelegramMap() {
        return telegramMap;
    }
//
    public List<String> getTelegramKeywordList() {
        return telegramKeywordList;
    }

    /**
     * //TODO ?????? ???????????? ?????? ????????? ???????????? ???..
     * @param maps
     * @return
     */
    private List<User> covert(List<Map<String, Object>> maps,List<Map<String, Object>> bots) {
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
        Map<String,Bot> botMap = convertToBot(bots);
        List<User> users = new ArrayList<>();
        for (String userId : map.keySet()) {
            List<Map<String, Object>> mapList = map.get(userId);
            int chatId = findChatId(mapList.get(0));
            if (chatId == 0) {
                continue;
            }
            User user = convertToUser(mapList, userId);
            setupBot(user, botMap);
            log.info("??????????????? <<< "+user);
            users.add(user);
        }

        Collections.sort(users, new Comparator<User>() {
            @Override
            public int compare(User o1, User o2) {
                return Integer.valueOf(o1.getGrade()).compareTo(Integer.valueOf(o2.getGrade()));
            }
        });
        return users;
    }

    private void setupBot(User user, Map<String, Bot> botMap) {
        String seq = user.getBotSeq() + "";
        if (botMap.containsKey(seq)) {
            Bot bot = botMap.get(seq);
            user.setBot(bot);
        }
    }

    private Map<String, Bot> convertToBot(List<Map<String, Object>> bots) {
        Map<String, Bot> map = new HashMap<>();
        for (Map<String, Object> bot : bots) {
            String seq = Maps.getValue(bot, "seq");
            String name = Maps.getValue(bot, "name");
            String token = Maps.getValue(bot, "token");
            map.put(seq, new Bot(seq, name, token));
        }

        return map;

    }
    private User convertToUser(List<Map<String, Object>> maps,String userId) {
        User user = new User();
        user.setUserid(userId);
        user.setGrade(findGrade(maps.get(0)));
        user.setChatId(findChatId(maps.get(0)));
        user.setBotSeq(findBotSeq(maps.get(0)));
        List<Keyword> keywords = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            String keyword = Maps.getValue(map, "keyword");
            keywords.add(new Keyword(keyword, 1));
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

    private long findBotSeq(Map<String, Object> map) {
        return Maps.getLongValue(map, "bot_seq");
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

    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public String[] getTemplates() {
        return templates;
    }

    public String findInputValue(Message message) {
        byte[] bytes = message.getBody();
        return new String(bytes, Charset.forName("UTF-8"));
    }
    public String getHeader(Message message,String key) {
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

    public void refreshTelegramUserInfo() {
        List<Map<String,Object>> maps = templateMapper.findUsers();
        List<Map<String,Object>> bots = templateMapper.findBots();
        List<User> users = covert(maps,bots);
//        List<Map<String,Object>> memberCodes = templateMapper.findMemberCode();
//        Map<String, List<String>> memberCodeMap = toMap(memberCodes);
        for (User user : users) {
//            if (memberCodeMap.containsKey(user.getUserid())) {
//                List<String> strings = memberCodeMap.get(user.getUserid());
//                for (String code : strings) {
//                    user.addCode(code);
//                }
//            }
            log.info("USER "+user.toString());
        }
        synchronized (telegramMap) {
            telegramMap.clear();
            for (User user : users) {
                telegramMap.put(user.getUserid(), user);
            }
        }
    }

    private Map<String, List<String>> toMap(List<Map<String,Object>> memberCodes) {
        Map<String, List<String>> map = new HashMap<>();
        for (Map<String, Object> code : memberCodes) {
            String userId = Maps.getValue(code, "userid");
            String isuCd = Maps.getValue(code, "isu_cd");
            if (isEmpty(isuCd)) {
                continue;
            }
            if (map.containsKey(userId)) {
                List<String> list = map.get(userId);
                list.add(isuCd);
            } else {
                List<String> list = new ArrayList<>();
                list.add(isuCd);
                map.put(userId, list);
            }
        }

        return map;
    }

    public boolean check(String queueName, Integer newCount) {
        boolean result = false;
        if (QUEUE_MAP.containsKey(queueName)) {
            int oldCount = QUEUE_MAP.get(queueName);
            if (oldCount == 0) {
                result =  true;
            } else {
                if (newCount - oldCount == 0) {
                    result =  false;
                } else {
                    result =  true;
                }
            }
        } else {
            // OK
            result =  true;
        }

        QUEUE_MAP.put(queueName, newCount);
        return result;
    }

    public boolean isKospi200(String code) {
        synchronized (KOSPI200) {
            return KOSPI200.containsKey(code);
        }
    }
}
