package com.tachyon.news.template.jobs;

import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import com.tachyon.crawl.kind.model.TelegramHolder;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.news.template.command.CommandFactory;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.TelegramBean;
import com.tachyon.news.template.model.User;
import com.tachyon.news.template.repository.TemplateMapper;
import com.tachyon.news.template.telegram.TelegramHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@EnableScheduling
public class NewsJobs {
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;

    @Autowired
    private CommandFactory commandFactory;
    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private ExecutorService noGroupThreadPool;

    private int workingStartHour = 7;
    private int workingEndHour = 20;

    // 그룹텔레그램 처리시 주기별 처리 갯수.. (분당20개, 30초당10개, 10개보다 작은 9개)
    private int countPerProcessing = 9;

    private Date publishDate;
    private Date krPublishDate;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Scheduled(cron = "0 0 7-20 ? * MON-FRI")
    public void setupTelegramInfo() {
        myContext.refreshTelegramKeywordList();
        myContext.refreshTelegramUserInfo();
    }


    /**
     * 주기적으로 속보키워드를 가진 공시 정보를 추출한다.
     * chat_id가 0보다 큰 것만 처리하게 쓰레드풀을 29개로 하여 순차적으로 처리하면 딱히 문제될 게 없어 보임.
     * 초당 30개 최대.
     * 개인 사용자에게는 전송하지 않음
     */
//    @Scheduled(fixedDelay = 30000)
    @Deprecated
    public void handleNoGroupTelegramHolder() {
        if (myContext.isDev()) {
            return;
        }
        //a.doc_no,a.keyword,a.isu_cd,a.acpt_no,b.doc_nm,b.doc_url,b.rpt_nm
        //7 - 20 까지만 처리하게 ..
        TelegramHandler noGroupTelegramHandler = new TelegramHandler() {
            @Override
            public boolean isChargedChatId(long chatId) {
                if (chatId > 0) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public List<Map<String, Object>> findTelegramHodler(TemplateMapper templateMapper) {
                return templateMapper.findNoGroupTelegramHolder();
            }

            @Override
            public void completeTelegramHolder(TemplateMapper templateMapper, String docNo, String acptNo, String keyword) {
                templateMapper.completeNoGroupTelegramHolder(docNo, acptNo, keyword);
            }

            @Override
            public String findName() {
                return "NoGroup";
            }

            @Override
            public void execute(List<Future> futures, TelegramBean telegramBean) {

                TelegramHelper telegramHelper = (TelegramHelper) commandFactory.findBean(TelegramHelper.class);
                futures.add(noGroupThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        handleTelegram(telegramBean, telegramHelper, findName());
                    }
                }));

            }

            @Override
            public boolean isNoGroup() {
                return true;
            }
        };

        //그룹외에는 처리하지 않는다.
//        handleTelegram(noGroupTelegramHandler);


    }

//    private void testTelegram() {
//        try {
//            List<Map<String, Object>> maps = templateMapper.findNoGroupTelegramHolder();
//            if (maps == null || maps.size() == 0) {
//                return;
//            }
//            log.info("... "+maps.size());
//            Collections.sort(maps, new Comparator<Map<String, Object>>() {
//                @Override
//                public int compare(Map<String, Object> o1, Map<String, Object> o2) {
//                    Timestamp timestamp1 = (Timestamp) o1.get("created_at");
//                    Timestamp timestamp2 = (Timestamp) o2.get("created_at");
//                    return timestamp1.compareTo(timestamp2);
//                }
//            });
//            List<TelegramHolder> holders = toTelegramHolder(maps);
//            TelegramHelper telegramHelper = (TelegramHelper) commandFactory.findBean(TelegramHelper.class);
//            for (TelegramHolder holder : holders) {
//                log.info(".. "+holder.getKeyword());
//                telegramHelper.sendToTelegram(new User(),holder);
//            }
//
//        } catch (Exception e) {
//            log.error(e.getMessage(),e);
//        }
//
//    }

    /**
     * chat_id가 0보다 작은 것을 대상으로 한다.
     * 조회시에 최대 9개를 수집해서 처리한다.
     * 그룹은 분당 20개가 최대...
     * 쓰레드풀 9개
     */
    @Scheduled(fixedDelay = 30000)
    public void handleGroupTelegramHolder() {
        if (myContext.isDev()) {
            return;
        }
        TelegramHandler groupTelegramHandler = new TelegramHandler() {
            @Override
            public boolean isChargedChatId(long chatId) {
                if (chatId < 0) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public List<Map<String, Object>> findTelegramHodler(TemplateMapper templateMapper) {
                List<Map<String, Object>> maps = templateMapper.findGroupTelegramHolder();
                if (maps == null || maps.size() == 0) {
                    return maps;
                }

                Collections.sort(maps, new Comparator<Map<String, Object>>() {
                    @Override
                    public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                        Timestamp timestamp1 = (Timestamp) o1.get("created_at");
                        Timestamp timestamp2 = (Timestamp) o2.get("created_at");
                        return timestamp1.compareTo(timestamp2);
                    }
                });

                if (maps.size() > countPerProcessing) {
                    List<Map<String, Object>> mapList = new ArrayList<>();
                    for (int i = 0; i < countPerProcessing; i++) {
                        Map<String, Object> map = maps.get(i);
                        mapList.add(map);
                    }

                    return mapList;
                } else {
                    return maps;
                }
            }

            @Override
            public void completeTelegramHolder(TemplateMapper templateMapper, String docNo, String acptNo, String keyword) {
                templateMapper.completeGroupTelegramHolder(docNo, acptNo, keyword);
            }

            @Override
            public String findName() {
                return "Group";
            }

            @Override
            public void execute(List<Future> futures, TelegramBean telegramBean) {
                TelegramHelper telegramHelper = (TelegramHelper) commandFactory.findBean(TelegramHelper.class);
                handleTelegram(telegramBean, telegramHelper, findName());
            }

            @Override
            public boolean isNoGroup() {
                return false;
            }
        };

        handleTelegram(groupTelegramHandler);
    }

    /**
     * 텔레그램 처리 기본 로직..
     *
     * @param telegramHandler 그룹방,비그룹방 처리를 표현하는 인터페이스
     */
    private void handleTelegram(TelegramHandler telegramHandler) {
        try {
            if (isWorkingHour(new Date()) == false) {
                log.debug("처리시간이 아님... ");
                return;
            }
            InfixToPostfixParens infix = new InfixToPostfixParens();

            List<Map<String, Object>> maps = telegramHandler.findTelegramHodler(templateMapper);
            if (maps == null || maps.size() == 0) {
//                log.debug(telegramHandler.findName()+" 처리할 속보데이터..가 없음..");
                return;
            } else {
                List<TelegramHolder> holders = toTelegramHolder(maps);
                log.info(telegramHandler.findName() + " 텔레그램 속보 갯수 " + holders.size());
                Map<String, User> telegramMap = myContext.getTelegramMap();
                // 결과저장.
                List<TelegramBean> telegramBeans = new ArrayList<>();

                synchronized (telegramMap) {
                    User admin = telegramMap.get("admin");
                    for (String userId : telegramMap.keySet()) {
                        User user = telegramMap.get(userId);
                        if ("admin".equalsIgnoreCase(user.getUserid())) {
                            continue;
                        }
                        if (telegramHandler.isChargedChatId(user.getChatId()) == false) {
                            continue;
                        }

                        findKeyword(user, admin, holders, telegramBeans, infix);
                        findStockChange(user, holders, telegramBeans);
                        findRelatives(user, holders, telegramBeans);
                    }
                }

                List<Future> futures = new ArrayList<>();
                // 텔레그램 전송...
                long start = System.nanoTime();
                if (telegramBeans.size() != 0) {
                    // 맨 마지막이라는 걸 알려주는 객체 삽입..
                    telegramBeans.add(new TelegramBean(null, null));
                }
                for (TelegramBean telegramBean : telegramBeans) {
                    //  봇당 30건에 마다 1초 지났는지 체크하고 지나지 않았으면 잠시 멈춤.
                    telegramHandler.execute(futures, telegramBean);
                }

                if (futures.size() > 0) {
                    while (true) {
                        try {
                            if (allDone(futures)) {
                                break;
                            }
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            log.error(e.getMessage());
                        }
                    }
                }
//                log.info(telegramHandler.findName() + " ALL ... " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)+" "+telegramBeans.size());

                // TelegramHolder 처리
                for (TelegramHolder holder : holders) {
                    telegramHandler.completeTelegramHolder(templateMapper, holder.getDocNo(), holder.getAcptNo(), holder.getKeyword());
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }

    }

    private void findRelatives(User user, List<TelegramHolder> holders, List<TelegramBean> telegramBeans) {
        for (TelegramHolder holder : holders) {
            if ("RELATIVE".equalsIgnoreCase(holder.getKeyword())) {
                TelegramBean bean = new TelegramBean(user,holder);
                log.info("RELATIVE <<< "+bean);
                telegramBeans.add(bean);
            } else {
                continue;
            }
        }
    }

    private void findStockChange(User user, List<TelegramHolder> holders, List<TelegramBean> telegramBeans) {
        for (TelegramHolder holder : holders) {
            if ("NOT_KEYWORD".equalsIgnoreCase(holder.getKeyword())) {
                telegramBeans.add(new TelegramBean(user,holder));
            } else {
                continue;
            }
        }
    }

    private boolean allDone(List<Future> list) {
        for (Future future : list) {
            if (future.isDone() == false) {
                return false;
            }
        }

        return true;
    }

    /**
     * 텔레그램 그룹,비그룹 처리를 구분하기 위한 인터페이스.
     */
    private interface TelegramHandler {
        /**
         * chatId가 0보다 크면 그룹용
         * 0보다 작으면 비그룹용
         *
         * @param chatId
         * @return
         */
        boolean isChargedChatId(long chatId);

        /**
         * 처리 데이터 수집.
         *
         * @param templateMapper
         * @return
         */
        List<Map<String, Object>> findTelegramHodler(TemplateMapper templateMapper);

        /**
         * 처리후 결과 처리.
         *
         * @param templateMapper
         * @param docNo
         * @param acptNo
         * @param keyword
         */
        void completeTelegramHolder(TemplateMapper templateMapper, String docNo, String acptNo, String keyword);

        /**
         * 이름 반환
         *
         * @return
         */
        String findName();

        /**
         * 처리를 리니어하게 할 지... 쓰레드풀로 처리할 지 구현할 수 있다.
         *
         * @param futures
         * @param telegramBean
         */
        void execute(List<Future> futures, TelegramBean telegramBean);

        boolean isNoGroup();
    }

    /**
     * 7시부터 19시59분까지 처리함.
     *
     * @param date
     * @return
     */
    private boolean isWorkingHour(Date date) {
        String hour = DateUtils.toString(date, "HH");
        int _hour = Integer.valueOf(hour);
        if (_hour >= workingStartHour && _hour < workingEndHour) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * 사용자별로 텔레그램Holder 처리함.
     *
     * @param user
     * @param admin
     * @param holders
     * @param telegramBeans
     */
    private void findKeyword(User user, User admin, List<TelegramHolder> holders, List<TelegramBean> telegramBeans, InfixToPostfixParens infix) {
        String userId = user.getUserid();
        for (TelegramHolder holder : holders) {
            if ("NOT_KEYWORD".equalsIgnoreCase(holder.getKeyword()) || "RELATIVE".equalsIgnoreCase(holder.getKeyword())) {
                continue;
            }
            // 사용자별 속보 키워드가 모두 동일하므로 아래 처러 admin으로 처리함.
            // TODO 사용자별 속보 키워드를 가지게 되면 아래 로직 변경 필요.
            if (user.hasCode(holder.getIsuCd())) {
                // 자신의 종목코드에 해당할 때.. (grade가 0이면 모두 받기)
                if (admin.hasKeyword(holder.getKeyword())) {
                    telegramBeans.add(new TelegramBean(user,holder));
                } else {

                }
            } else {
                log.info(userId + "의 속보종목 공시가 아님. " + holder.getIsuCd());
            }
        }
    }


    private List<TelegramHolder> toTelegramHolder(List<Map<String, Object>> maps) {
        List<TelegramHolder> holders = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            holders.add(TelegramHolder.from(map));
        }
        return holders;
    }

    private void handleTelegram(TelegramBean telegramBean, TelegramHelper telegramHelper, String name) {
//        long start = System.nanoTime();
        telegramHelper.sendToTelegram(telegramBean.getUser(),telegramBean.getTelegramHolder());
//        log.info(name + " ONE ... " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
    }

    /**
     * rabbitmq 모니터링..
     */
    @Scheduled(fixedDelay = 60000)
    public void checkRabbitMq() {
        if (isWorkingHour(new Date()) == false) {
            return;
        }
        // 관심있는 큐이름 메세지 갯수를 체크..
        // 1분마다 갯수의 차이를 확인..
        // 차이가 발생하지 않으면 노티한다.
        String[] names = new String[]{"GATEWAY", "OK_GATEWAY", "KONGSI_CHECK", "META_KONGSI", "_SPLIT_QUEUE"};
        List<String> temp = new ArrayList<>();
        for (String name : names) {
            int check = checkQueue(name);
            if (check == -1) {
                log.warn("NO QUEUE " + name);
            } else {
                if (check == 0) {
                } else {
                    log.error("큐의 갯수가 동일함. " + check + " " + name);
                    temp.add(name + "_" + check);
                }
            }
        }

        // 334618638
        if (temp.size() > 0) {
            sendToMonitoringBot(temp);
        }

    }

    private void sendToMonitoringBot(List<String> temp) {
        TelegramHelper telegramHelper = (TelegramHelper) commandFactory.findBean(TelegramHelper.class);
        telegramHelper.sendToMonitoringBot(temp);
    }


    private int checkQueue(String name) {
        Properties properties = amqpAdmin.getQueueProperties(name);
        if (properties == null) {
            return -1;
        } else {
            int count = (Integer) properties.get("QUEUE_MESSAGE_COUNT");
            if (myContext.check(name, count) == false) {
                return count;
            } else {
                return 0;
            }
        }
    }

    /**
     * clinicaltrials 10분마다 모니터링.. 프로젝트 시작까지 잠시 멈춤.
     */
//    @Scheduled(fixedDelay = 1000 * 60 * 10)
    public void monitorClinicalTrialsUpdateDateTime() {
        try {
            URL feedSource = new URL("https://clinicaltrials.gov/ct2/results/rss.xml?rcv_d=&lup_d=14&sel_rss=mod14&rslt=With&count=1");
            SyndFeedInput input = new SyndFeedInput();
            SyndFeed feed = input.build(new XmlReader(feedSource));
            Date date = feed.getPublishedDate();
            if (publishDate == null) {
                publishDate = date;
                log.info("PUB_DATE  => "+toString(date));
                monitorRssPubDate(date);
            } else {
                if (publishDate.equals(date)) {

                } else {
                    log.info("PUB_DATE "+toString(publishDate) + " => " + toString(date));
                    publishDate = date;
                    monitorRssPubDate(date);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }
    /**
     * 한국 clinicaltrials 10분마다 모니터링..
     */
    @Scheduled(fixedDelay = 1000 * 60 * 10)
    public void monitorKrClinicalTrialsUpdateDateTime() {
        try {
            if (myContext.isDev()) {
                return;
            }
            URL feedSource = new URL("https://clinicaltrials.gov/ct2/results/rss.xml?rcv_d=&lup_d=14&sel_rss=mod14&count=1&cntry=KR");
            SyndFeedInput input = new SyndFeedInput();
            SyndFeed feed = input.build(new XmlReader(feedSource));
            Date date = feed.getPublishedDate();
            if (krPublishDate == null) {
                krPublishDate = date;
                log.info("KRPUB_DATE  => "+toString(date));
                monitorKrRssPubDate(date);
            } else {
                if (krPublishDate.equals(date)) {

                } else {
                    log.info("KRPUB_DATE "+toString(krPublishDate) + " => " + toString(date));
                    krPublishDate = date;
                    monitorKrRssPubDate(date);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    private void monitorClinicalTrialsUpdateDateTime(String url) {
        //https://clinicaltrials.gov/ct2/results/rss.xml?rcv_d=&lup_d=14&sel_rss=mod14&count=1&cntry=KR
    }

    private void monitorRssPubDate(Date date) {
        String rssPubDate =  toString2(date);
        int count = templateMapper.findRssPubDateCount(rssPubDate);
        if (count == 0) {
            templateMapper.insertRssPubDate(rssPubDate);
            rabbitTemplate.convertAndSend("CRINICAL_TRIALS_BATCH",rssPubDate);
            log.info("CRINICAL_TRIALS_BATCH <<< "+rssPubDate);
        }
    }
    private void monitorKrRssPubDate(Date date) {
        String rssPubDate =  toString2(date);
        int count = templateMapper.findKrRssPubDateCount(rssPubDate);
        if (count == 0) {
            templateMapper.insertKrRssPubDate(rssPubDate);
            rabbitTemplate.convertAndSend("KR_CRINICAL_TRIALS_BATCH",rssPubDate);
            log.info("KR_CRINICAL_TRIALS_BATCH <<< "+rssPubDate);
        }
    }
    private String toString(Date date) {
        return DateUtils.toString(date, "yyyy-MM-dd HH:mm:ss");
    }
    private String toString2(Date date) {
        return DateUtils.toString(date, "yyyyMMddHHmmss");
    }


    @Scheduled(cron = "0 0 7 ? * *")
    public void refreshKospi200() {
        myContext.refreshKospi200();
    }


}
