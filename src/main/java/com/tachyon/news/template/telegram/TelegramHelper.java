package com.tachyon.news.template.telegram;

import com.github.mustachejava.Mustache;
import com.tachyon.crawl.kind.model.TelegramHolder;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.TelegramSender;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.Limit;
import com.tachyon.news.template.model.User;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.ReplyKeyboardRemove;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.io.StringWriter;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Component
public class TelegramHelper {
    @Autowired
    private TachyonMonitoringBot tachyonMonitoringBot;
    @Autowired
    private Mustache mustache;
    @Autowired
    private Mustache investmentMustache;
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;
    @Autowired
    private CloseableHttpAsyncClient asyncClient;

    private Map<String, Limit> limitMap = new HashMap<>();

    /**
     * @param user
     * @param telegramHolder
     */
    public void sendToTelegram(User user, TelegramHolder telegramHolder) {
        if (user == null || telegramHolder == null) {
            limitMap.clear();
            return;
        }
        if ("NOT_KEYWORD".equalsIgnoreCase(telegramHolder.getKeyword())) {
            sendChangeFlashToTelegram(user, telegramHolder);
        } else {
            sendKeywordFlashToTelegram(user, telegramHolder);
        }

    }

    private void sendKeywordFlashToTelegram(User user, TelegramHolder telegramHolder) {
        try {
            String keyword = telegramHolder.getKeyword();
            String acptNm = telegramHolder.getAcptNm();
            String acptNo = telegramHolder.getAcptNo();
            String name = findCompanyName(telegramHolder.getIsuCd());
            String docNm = telegramHolder.getDocNm();
            String docUrl = telegramHolder.getDocUrl();
            String tndDt = telegramHolder.getTndDt();
            String _message = makeMessage(keyword, acptNm, getItemUrl(acptNo), name, docNm, docUrl, tndDt);
            String key = telegramHolder.getDocNo() + "_" + telegramHolder.getIsuCd() + "_" + telegramHolder.getAcptNo();
            sendAsync(user, _message, key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void sendChangeFlashToTelegram(User user, TelegramHolder telegramHolder) {
        // change 정보를 수집하고 메세지를 만들어 텔레그램으로 전송함.
        try {

            String docNo = telegramHolder.getDocNo();
            String code = telegramHolder.getIsuCd();
            String acptNo = telegramHolder.getAcptNo();


            List<Map<String,Object>> maps = templateMapper.findChangeWithTelegram(docNo, code, acptNo);
            if (maps == null || maps.size() == 0) {
                return;
            }

            String name = findCompanyName(telegramHolder.getIsuCd());
            String acptNm = telegramHolder.getAcptNm();
            String docNm = telegramHolder.getDocNm();
            String docUrl = telegramHolder.getDocUrl();
            String tndDt = telegramHolder.getTndDt();
            String acptUlr = getItemUrl(acptNo);
            String key = telegramHolder.getDocNo() + "_" + telegramHolder.getIsuCd() + "_" + telegramHolder.getAcptNo();
            for (Map<String, Object> map : maps) {
                String ownerName = Maps.getValue(map,"owner_name");
                String before = Maps.getValue(map,"before_amt");
                String after = Maps.getValue(map,"after_amt");
                String price = Maps.getValue(map,"unit_price");
                String sum = findSum(before,after,price);
                String _message = makeInvestmentMessage(ownerName, tndDt, name, before, after, price, sum, docUrl, docNm, acptUlr, acptNm);
                sendAsync(user, _message, key);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private String findSum(String before, String after, String price) {
        long b = Long.valueOf(before);
        long a = Long.valueOf(after);
        long p = Long.valueOf(price);
        long s = (-1) * (a-b)*p;
        return s + "";
    }


    /**
     *                 "봇이름: tachyonnews_mining_bot\n" +
     *                         "투자자: <b>{{investor}}</b>\n" +
     *                         "발생시간: {{time}}\n" +
     *                         "종목: <b>{{company}}</b>\n" +
     *                         "변동전: <b>{{before}}</b>\n" +
     *                         "변동후: <b>{{after}}</b>\n" +
     *                         "단가: <b>{{price}}</b>\n" +
     *                         "실현액: <b>{{sum}}</b>\n" +
     *                         "공시명: <a href=\"{{docUrl}}\">{{docNm}}</a>\n" +
     *                         "기초공시명: <a href=\"{{acptUrl}}\">{{acptNm}}</a>";
     * @return
     */
    private String makeInvestmentMessage(String investor,String time,String company,String before,String after,String price,String sum,String docUrl,String docNm,String acptUrl,String acptNm) {
        StringWriter message = new StringWriter();
        investmentMustache.execute(message, investmentScopes(investor,time,company,before,after,price,sum,docUrl,docNm,acptUrl,acptNm));
        return message.toString();
    }

    private Map<String, Object> investmentScopes(String investor,String time,String company,String before,String after,String price,String sum,String docUrl,String docNm,String acptUrl,String acptNm) {
        Map<String, Object> scopes = new HashMap<String, Object>();
        scopes.put("investor", investor);
        scopes.put("time", time);
        scopes.put("company", company);
        scopes.put("before", numberFormat(before));
        scopes.put("after", numberFormat(after));
        scopes.put("price", numberFormat(price));
        scopes.put("sum", numberFormat(sum));
        scopes.put("docUrl", docUrl);
        scopes.put("docNm", docNm);
        scopes.put("acptUrl", acptUrl);
        scopes.put("acptNm", acptNm);
        return scopes;
    }


    private String numberFormat(String value) {
        DecimalFormat decimalFormat = new DecimalFormat("#,###.##");
        return decimalFormat.format(Double.valueOf(value));
    }
    private void sendAsync(User user,String _message,String key) throws Exception {
        String _chatId = user.getChatId() + "";
        long start = System.currentTimeMillis();
        if (loadBalancerCommandHelper != null) {
            loadBalancerCommandHelper.executeAsync(makeUrl(user.getBot().getToken(), _chatId, _message), makeCallback(key, user.getUserid(), _chatId, _message), asyncClient);
        } else {
            TelegramSender.sendAsync(makeUrl(user.getBot().getToken(), _chatId, _message), makeCallback(key, user.getUserid(), _chatId, _message), asyncClient);
        }
        long end = System.currentTimeMillis();
        handleTelegramLimit(user.getBot().getToken(), end - start);
    }
    /**
     * token에 해당하는 봇의 Limit 처리..
     * 초당 30건만 처리해야 함.
     * 실제는 초당 29건만 처리가능하게 함.
     * @param token
     * @param time
     * @throws InterruptedException
     */
    private void handleTelegramLimit(String token, long time) throws InterruptedException {
        if (limitMap.containsKey(token)) {
            Limit limit = limitMap.get(token);
            long sleep = limit.doExecute(time);
            if (sleep > 0) {
                log.info("SLEEP " + token + " " + sleep);
                plusTime(limitMap, token, sleep);
            }

        } else {
            limitMap.put(token, new Limit(1, time));
        }
    }

    /**
     * 다른봇을 위해 잠시 멈춘 시간을 보정함..
     * @param limitMap
     * @param token
     * @param sleep
     */
    private void plusTime(Map<String, Limit> limitMap, String token, long sleep) {
        for (String key : limitMap.keySet()) {
            if (key.equalsIgnoreCase(token)) {
                continue;

            }
            Limit limit = limitMap.get(key);
            limit.plusTime(sleep);
        }
    }

    private FutureCallback<HttpResponse> makeCallback(String key, String userId, String chatId, String _message) {
        return new FutureCallback<HttpResponse>() {
            public void completed(final HttpResponse response2) {
                log.info(key + " OK >>> " + userId + " " + chatId + " " + removeNewLine(_message));
            }

            public void failed(final Exception ex) {
                log.info(key + " FAIL >>> " + ex.getMessage() + " " + userId + " " + chatId + " " + removeNewLine(_message));
            }

            public void cancelled() {
                log.info(key + " CANCEL >>> " + userId + " " + chatId + " " + removeNewLine(_message));
            }
        };
    }

    private String makeUrl(String token, String chatId, String message) throws URISyntaxException {
        URIBuilder ub = new URIBuilder("https://api.telegram.org/bot" + token + "/sendMessage");
        ub.addParameter("chat_id", chatId);
        ub.addParameter("text", message);
        ub.addParameter("parse_mode", "HTML");
        // 아래는 삭제해야 함..
//        ub.addParameter("reply_markup", "ReplyKeyboardRemove");
        return ub.toString();
    }

    private String makeMessage(String keyword, String rptNm, String acptUrl, String name, String docNm, String docUlr, String tnsDt) {
        StringWriter message = new StringWriter();
        mustache.execute(message, scopes(tnsDt, keyword, name, docUlr, docNm, acptUrl, rptNm));
        return message.toString();
    }

    private Map<String, Object> scopes(String time, String keyword, String company, String docUrl, String docNm, String acptUrl, String acptNm) {
        Map<String, Object> scopes = new HashMap<String, Object>();
        scopes.put("time", time);
        scopes.put("keyword", keyword);
        scopes.put("company", company);
        scopes.put("docUrl", docUrl);
        scopes.put("docNm", docNm);
        scopes.put("acptUrl", acptUrl);
        scopes.put("acptNm", acptNm);
        return scopes;
    }

    private String removeNewLine(String value) {
        return StringUtils.remove(value, "\n");
    }

    private String getItemUrl(String acptNo) {
        return "http://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno=" + acptNo;
    }

    private String findCompanyName(String code) {
        String name = myContext.findComName(code);
        if (isEmpty(name)) {
            name = templateMapper.findName(code);
            if (isEmpty(name) == false) {
                myContext.putCompany(code, name);
            }
        }
        return name;
    }

    private boolean isEmpty(String value) {
        if (value == null || "".equalsIgnoreCase(value)) {
            return true;
        } else {
            return false;
        }
    }

    public void sendToMonitoringBot(List<String> temp) {

        try {
            SendMessage sendMessage = new SendMessage();
            sendMessage.setChatId("334618638");
            sendMessage.enableHtml(false);
            sendMessage.setText(makeMonitoringMsg(temp));
            sendMessage.setReplyMarkup(new ReplyKeyboardRemove());
            tachyonMonitoringBot.execute(sendMessage);
        } catch (TelegramApiException e) {
            log.error(e.getMessage() + " MONITORING.. ");
        }
    }

    private String makeMonitoringMsg(List<String> temp) {
        StringBuilder builder = new StringBuilder();
        builder.append("MonitoringBot..").append("\n");
        builder.append(DateUtils.toDTType()).append("\n");
        for (String queue : temp) {
            builder.append(queue).append("\n");
        }
        return builder.toString().trim();
    }

}
