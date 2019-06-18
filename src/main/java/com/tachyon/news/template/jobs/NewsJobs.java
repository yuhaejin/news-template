package com.tachyon.news.template.jobs;

import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.news.template.command.CommandFactory;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.TelegramBean;
import com.tachyon.news.template.model.TelegramHolder;
import com.tachyon.news.template.model.User;
import com.tachyon.news.template.repository.TemplateMapper;
import com.tachyon.news.template.telegram.TelegramHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
    private ExecutorService groupThreadPool;
    @Autowired
    private ExecutorService noGroupThreadPool;

    private int workingStartHour = 7;
    private int workingEndHour = 20;

    @Scheduled(cron = "0 0 7-20 ? * MON-FRI")
    public void setupTelegramInfo() {
        myContext.refreshTelegramKeywordList();
        myContext.refreshTelegramUserInfo();
    }


    /**
     * 주기적으로 속보키워드를 가진 공시 정보를 추출한다.
     * chat_id가 0보다 큰 것만 처리하게 쓰레드풀을 29개로 하여 순차적으로 처리하면 딱히 문제될 게 없어 보임.
     * 초당 30개 최대.
     */
    @Scheduled(fixedDelay = 30000)
    public void handleNoGroupTelegramHolder() {
        //a.doc_no,a.keyword,a.isu_cd,a.acpt_no,b.doc_nm,b.doc_url,b.rpt_nm
        //7 - 20 까지만 처리하게 ..
        TelegramHandler groupTelegramHandler = new TelegramHandler() {
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
            public ExecutorService findThreadPool() {
                return noGroupThreadPool;
            }

            @Override
            public String findName() {
                return "NoGroup";
            }
        };

        handleTelegram(groupTelegramHandler);


    }

    /**
     * chat_id가 0보다 작은 것을 대상으로 한다.
     * 조회시에 최대 9개를 수집해서 처리한다.
     * 그룹은 분당 20개가 최대...
     * 쓰레드풀 9개
     */
    @Scheduled(fixedDelay = 30000)
    public void handleGroupTelegramHolder() {
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
                //TODO 9개만 처리할 수 있도록 해야 함.
                // 날짜로 정렬하여 9개만 반환하게 하면 됨.
                return templateMapper.findGroupTelegramHolder();
            }

            @Override
            public void completeTelegramHolder(TemplateMapper templateMapper, String docNo, String acptNo, String keyword) {
                templateMapper.completeGroupTelegramHolder(docNo, acptNo, keyword);
            }

            @Override
            public ExecutorService findThreadPool() {
                return groupThreadPool;
            }

            @Override
            public String findName() {
                return "Group";
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
        if (isWorkingHour(new Date()) == false) {
            return;
        }
        InfixToPostfixParens infix = new InfixToPostfixParens();

        List<Map<String, Object>> maps = telegramHandler.findTelegramHodler(templateMapper);
        if (maps == null || maps.size() == 0) {
            return;
        } else {
            List<TelegramHolder> holders = toTelegramHolder(maps);
            log.info(telegramHandler.findName()+" 텔레그램 속보 갯수 " + holders.size());
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
                }
            }

            List<Future> futures = new ArrayList<>();
            TelegramHelper telegramHelper = (TelegramHelper) commandFactory.findBean(TelegramHelper.class);
            // 텔레그램 전송...
            long start = System.nanoTime();
            ExecutorService threadPoll = telegramHandler.findThreadPool();
            for (TelegramBean telegramBean : telegramBeans) {
                futures.add(threadPoll.submit(new Runnable() {
                    @Override
                    public void run() {
                        handleTelegram(telegramBean, telegramHelper,telegramHandler.findName());
                    }
                }));

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
            log.info(telegramHandler.findName()+" ALL ... " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));

            // TelegramHolder 처리
            for (TelegramHolder holder : holders) {
                telegramHandler.completeTelegramHolder(templateMapper, holder.getDocNo(), holder.getAcptNo(), holder.getKeyword());
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
    private interface TelegramHandler {
        boolean isChargedChatId(long chatId);

        List<Map<String, Object>> findTelegramHodler(TemplateMapper templateMapper);

        void completeTelegramHolder(TemplateMapper templateMapper, String docNo, String acptNo, String keyword);

        ExecutorService findThreadPool();
        String findName();
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
            // 사용자별 속보 키워드가 모두 동일하므로 아래 처러 admin으로 처리함.
            // TODO 사용자별 속보 키워드를 가지게 되면 아래 로직 변경 필요.
            if (user.hasCode(holder.getIsuCd())) {
                // 자신의 종목코드에 해당할 때.. (grade가 0이면 모두 받기)
                if (admin.hasKeyword(holder.getKeyword())) {
                    telegramBeans.add(new TelegramBean(userId, user.getChatId(), holder));
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

    private void handleTelegram(TelegramBean telegramBean, TelegramHelper telegramHelper,String name) {
        long start = System.nanoTime();
        telegramHelper.sendToTelegram(telegramBean.getUserId(), telegramBean.getChatId(), telegramBean.getTelegramHolder());
        log.info(name+" ONE ... " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
    }

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


}
