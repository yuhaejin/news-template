package com.tachyon.news.template.telegram;

import com.github.mustachejava.Mustache;
import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.TelegramHolder;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.TelegramSender;
import com.tachyon.helper.MustacheHelper;
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

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;


@Slf4j
@Component
public class TelegramHelper {
    @Autowired
    private TachyonMonitoringBot tachyonMonitoringBot;
    @Autowired
    private Mustache keywordMustache;
    @Inject
    LinkedHashMap<String,Mustache> plusInvestmentMustaches;
    @Inject
    LinkedHashMap<String,Mustache>  minusInvestmentMustaches;

    @Autowired
    private Mustache investmentMustache;
    @Autowired
    private Mustache relativeMustache;
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;
    @Autowired
    private CloseableHttpAsyncClient asyncClient;

    private Map<String, Limit> limitMap = new HashMap<>();


    private Random random = new Random(System.currentTimeMillis());
    @PostConstruct
    public void debug() {
    }
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
        }else if("RELATIVE".equalsIgnoreCase(telegramHolder.getKeyword())){
            sendRelativeToTelegram(user, telegramHolder);
        } else {
            sendKeywordFlashToTelegram(user, telegramHolder);
        }

    }

    /**
     * ????????? ?????? ??????.
     * ????????? ????????? ?????? ?????? ????????? ???????????? ???????????? ?????????.
     * @param user
     * @param telegramHolder
     */
    private void sendRelativeToTelegram(User user, TelegramHolder telegramHolder) {
        try {
            log.info("TELEGRAM <<< "+user+" "+telegramHolder);
            String docNo = telegramHolder.getDocNo();
            String code = telegramHolder.getIsuCd();
            String acptNo = telegramHolder.getAcptNo();

            List<Map<String,Object>> maps = templateMapper.findRelativeWithTelegram(docNo, code, acptNo);
            if (maps == null || maps.size() == 0) {
                return;
            }
            // ?????????
            String name = findCompanyName(telegramHolder.getIsuCd());
            String acptNm = telegramHolder.getAcptNm();
            String docNm = telegramHolder.getDocNm();
            String docUrl = telegramHolder.getDocUrl();
            String tndDt = telegramHolder.getTndDt();
            String acptUlr = getItemUrl(acptNo);
            String key = telegramHolder.getDocNo() + "_" + telegramHolder.getIsuCd() + "_" + telegramHolder.getAcptNo();
            for (Map<String, Object> map : maps) {
                // ????????????
                String name2 = Maps.getValue(map,"name");
                // ????????? ??????
                String birth = Maps.getValue(map,"birth");

                List<Map<String, Object>> mapList  = templateMapper.findRelativeCount(docNo, code, acptNo, name2, birth);
                if (mapList.size() == 0) {
                    // ??? ?????????????????? ??????..
                    String gender = Maps.getValue(map,"gender");
                    String homeabroad = Maps.getValue(map,"homeabroad");
                    String nationality = Maps.getValue(map,"nationality");
                    String relationHim = Maps.getValue(map,"relation_him");
                    String address = Maps.getValue(map,"address");
                    String job = Maps.getValue(map,"job");
                    String _message = makeRelativeMessage(name,name2,birth, gender,homeabroad,nationality,relationHim,address,job,tndDt, docUrl, docNm, acptUlr, acptNm);
                    sendAsync(user, _message, key);
                } else {

                    continue;
                }

            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * ?????? ????????? ??????.
     * @param companyName ????????? ??????
     * @param name ????????? ??????
     * @param birth ??????
     * @param gender ???
     * @param homeabroad ??????/??????
     * @param nationality ??????
     * @param relationHim ??????
     * @param address ??????
     * @param job  ???????????? ??????
     * @param tndDt ???????????? ??????
     * @param docUrl ??????url
     * @param docNm ?????????
     * @param acptUrl ????????????url
     * @param acptNm ???????????????
     * @return
     */
    private String makeRelativeMessage(String companyName,String name, String birth, String gender, String homeabroad, String nationality, String relationHim, String address, String job, String tndDt, String docUrl, String docNm, String acptUrl, String acptNm) {

        StringWriter message = new StringWriter();
        relativeMustache.execute(message, relativeScopes(companyName,name,tndDt,birth,gender,homeabroad,nationality,relationHim,address,job,docUrl,docNm,acptUrl,acptNm));
        return message.toString();

    }

    private Map<String, Object> relativeScopes(String companyName,String name, String tndDt, String birth, String gender, String homeabroad, String nationality, String relationHim,String job, String relationCom, String docUrl, String docNm, String acptUrl, String acptNm) {
        Map<String, Object> scopes = new HashMap<String, Object>();
        scopes.put("company", companyName);
        scopes.put("time", tndDt);
        scopes.put("name", name);
        scopes.put("birth", birth);
        scopes.put("gender", gender);
        scopes.put("homeabroad", homeabroad);
        scopes.put("nationality", nationality);
        scopes.put("relationHim", relationHim);
        scopes.put("relationCom", relationCom);
        scopes.put("job", job);
        scopes.put("docUrl", docUrl);
        scopes.put("docNm", docNm);
        scopes.put("acptUrl", acptUrl);
        scopes.put("acptNm", acptNm);
        return scopes;
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

    /**
     * ?????? ?????? ??????..
     * @param user
     * @param telegramHolder ???????????? ????????? ?????? ?????? ??????..
     */
    private void sendChangeFlashToTelegram(User user, TelegramHolder telegramHolder) {
        // change ????????? ???????????? ???????????? ????????? ?????????????????? ?????????.
        try {
            log.info("... sendChangeFlashToTelegram");
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
            String acptUrl = getItemUrl(acptNo);

            String day = acptNo.substring(6, 8);
            String key = telegramHolder.getDocNo() + "_" + telegramHolder.getIsuCd() + "_" + telegramHolder.getAcptNo();
            for (Map<String, Object> map : maps) {
                String ownerName = Maps.getValue(map,"owner_name");
                String before = Maps.getValue(map,"before_amt");
                String after = Maps.getValue(map,"after_amt");
                String price = Maps.getValue(map,"unit_price");
                String birth = Maps.getValue(map,"birth_day");
                String stockMethod = Maps.getValue(map,"stock_type");
                stockMethod = MustacheHelper.modifyStockMethod(stockMethod);
                String interval = findInterval(before, after);
                if ("0".equalsIgnoreCase(interval)) {
                    log.info("SKIP ??????????????? ??????.. " + map);
                    continue;
                }

                String sum = findSum(before,after,price);
                String lastName = findLastName(ownerName);
                Map<String,Object> investmentScope = investmentScopes(ownerName,tndDt,name,before,after,price,sum,docUrl,docNm,acptUrl,acptNm,"");
                // ????????? ?????? ??????
                addInvestmentScope(investmentScope, day, birth, interval, lastName, before, after, price, stockMethod,sum,code);
                Mustache mustache = MustacheHelper.findMustache(plusInvestmentMustaches, minusInvestmentMustaches, different(before,after),random);
                String article = makeInvestmentMessage(mustache,investmentScope);
                String message = makeInvestmentMessage(ownerName, tndDt, name, before, after, price, sum, docUrl, docNm, acptUrl, acptNm,article);
                sendAsync(user, message, key);
                log.info("TELEGRAM  <<< "+article);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private long different(String before, String after) {
        return Long.valueOf(after) - Long.valueOf(before);
    }

    private void addInvestmentScope(Map<String, Object> investmentScope, String day, String birth, String interval, String lastName, String before, String after, String price, String stockMethod, String sum,String code) {
        MustacheHelper.addInvestmentScope(investmentScope,day,birth,interval,lastName,before,after,price,stockMethod,sum,code);
    }

    private String findLastName(String ownerName) {
        return ownerName.charAt(0)+"";
    }

    private String findInterval(String before, String after) {
        return Math.abs(Integer.valueOf(after) - Integer.valueOf(before))+"";
    }

//    private String makeInvestmentMessage(List<Mustache> plusInvestmentMustaches, List<Mustache> minusInvestmentMustaches, String sum, Map<String, Object> investmentScope) {
//        StringWriter message = new StringWriter();
//        Mustache mustache = findMustache(plusInvestmentMustaches, minusInvestmentMustaches, sum);
//        mustache.execute(message, investmentScope);
//        return message.toString();
//    }
    private String makeInvestmentMessage(Mustache mustache, Map<String, Object> investmentScope) {
        StringWriter message = new StringWriter();
        mustache.execute(message, investmentScope);
        return message.toString();
    }

    private String findSum(String before, String after, String price) {
        long b = Long.valueOf(before);
        long a = Long.valueOf(after);
        long p = Long.valueOf(price);
        long s = (-1) * (a-b)*p;
        return s + "";
    }


    /**
     *                 "?????????: tachyonnews_mining_bot\n" +
     *                         "?????????: <b>{{investor}}</b>\n" +
     *                         "????????????: {{time}}\n" +
     *                         "??????: <b>{{company}}</b>\n" +
     *                         "?????????: <b>{{before}}</b>\n" +
     *                         "?????????: <b>{{after}}</b>\n" +
     *                         "??????: <b>{{price}}</b>\n" +
     *                         "?????????: <b>{{sum}}</b>\n" +
     *                         "?????????: <a href=\"{{docUrl}}\">{{docNm}}</a>\n" +
     *                         "???????????????: <a href=\"{{acptUrl}}\">{{acptNm}}</a>";
     * @return
     */
    private String makeInvestmentMessage(String investor,String time,String company,String before,String after,String price,String sum,String docUrl,String docNm,String acptUrl,String acptNm,String article) {
        StringWriter message = new StringWriter();
        investmentMustache.execute(message, investmentScopes(investor,time,company,before,after,price,sum,docUrl,docNm,acptUrl,acptNm,article));
        return message.toString();
    }

    private Map<String, Object> investmentScopes(String investor,String time,String company,String before,String after,String price,String sum,String docUrl,String docNm,String acptUrl,String acptNm,String article) {
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
        scopes.put("article", article);
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
     * token??? ???????????? ?????? Limit ??????..
     * ?????? 30?????? ???????????? ???.
     * ????????? ?????? 29?????? ?????????????????? ???.
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
     * ???????????? ?????? ?????? ?????? ????????? ?????????..
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
        // ????????? ???????????? ???..
//        ub.addParameter("reply_markup", "ReplyKeyboardRemove");
        return ub.toString();
    }

    private String makeMessage(String keyword, String rptNm, String acptUrl, String name, String docNm, String docUlr, String tnsDt) {
        StringWriter message = new StringWriter();
        keywordMustache.execute(message, scopes(tnsDt, keyword, name, docUlr, docNm, acptUrl, rptNm));
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
