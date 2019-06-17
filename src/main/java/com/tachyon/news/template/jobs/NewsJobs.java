package com.tachyon.news.template.jobs;

import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.news.template.command.CommandFactory;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.TelegramBean;
import com.tachyon.news.template.model.TelegramHolder;
import com.tachyon.news.template.model.User;
import com.tachyon.news.template.repository.TemplateMapper;
import com.tachyon.news.template.telegram.TachyonMonitoringBot;
import com.tachyon.news.template.telegram.TelegramHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
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

    private int workingStartHour = 7;
    private int workingEndHour = 20;

    @Scheduled(cron = "0 0 7-20 ? * MON-FRI")
    public void setupTelegramInfo() {
        myContext.refreshTelegramKeywordList();
        myContext.refreshTelegramUserInfo();
    }

    /**
     * 주기적으로 속보키워드를 가진 공시 정보를 추출한다.
     */
    @Scheduled(fixedDelay = 30000)
    public void handleTelegramHolder() {
        //a.doc_no,a.keyword,a.isu_cd,a.acpt_no,b.doc_nm,b.doc_url,b.rpt_nm
        //7 - 20 까지만 처리하게 ..
        if (isWorkingHour(new Date())==false) {
            return;
        }

        String time = DateUtils.toDTType();

        List<Map<String,Object>> maps = templateMapper.findTelegramHolder();
        if (maps == null || maps.size() == 0) {
            return;
        } else {
            List<TelegramHolder> holders = toTelegramHolder(maps);
            log.info("텔레그램 속보 갯수 "+holders.size());
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
                    findKeyword(user, admin, holders, telegramBeans);
                }
            }

            TelegramHelper telegramHelper = (TelegramHelper)commandFactory.findBean(TelegramHelper.class);
            // 텔레그램 전송...
            for (TelegramBean telegramBean : telegramBeans) {
                handleTelegram(telegramBean,telegramHelper);
            }

            // TelegramHolder 처리
            for (TelegramHolder holder : holders) {
                templateMapper.completeTelegramHolder(holder.getDocNo(),holder.getAcptNo(),holder.getKeyword());
            }
        }


    }

    /**
     * 7시부터 19시59분까지 처리함.
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
     * @param user
     * @param admin
     * @param holders
     * @param telegramBeans
     */
    private void findKeyword(User user,User admin,List<TelegramHolder> holders,List<TelegramBean> telegramBeans) {
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
                log.info(userId+ "의 속보종목 공시가 아님. "+holder.getIsuCd());
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

    private void handleTelegram(TelegramBean telegramBean,TelegramHelper telegramHelper ) {
        long start = System.nanoTime();
        telegramHelper.sendToTelegram(telegramBean.getUserId(), telegramBean.getChatId(), telegramBean.getTelegramHolder());
        log.info("... " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-start,TimeUnit.NANOSECONDS) );
    }


    @Scheduled(fixedDelay = 60000)
    public void checkRabbitMq() {
        if (isWorkingHour(new Date())==false) {
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
                    log.error("큐의 갯수가 동일함. "+check+" "+name);
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
        TelegramHelper telegramHelper = (TelegramHelper)commandFactory.findBean(TelegramHelper.class);
        telegramHelper.sendToMonitoringBot(temp);
    }


    private int checkQueue(String name) {
        Properties properties = amqpAdmin.getQueueProperties(name);
        if (properties == null) {
            return -1;
        } else {
            int count = (Integer)properties.get("QUEUE_MESSAGE_COUNT");
            if (myContext.check(name, count) == false) {
                return count;
            } else {
                return 0;
            }
        }
    }


}
