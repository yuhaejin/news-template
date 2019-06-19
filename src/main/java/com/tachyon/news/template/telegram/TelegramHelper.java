package com.tachyon.news.template.telegram;

import com.github.mustachejava.Mustache;
import com.tachyon.crawl.kind.model.TelegramHolder;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.ReplyKeyboardRemove;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Component
public class TelegramHelper {
    @Autowired
    private TachyonNewsFlashBot tachyonNewsFlashBot;
    @Autowired
    private TachyonMonitoringBot tachyonMonitoringBot;
    @Autowired
    private Mustache mustache;
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;
    @Autowired
    private CloseableHttpAsyncClient asyncClient;

    public void sendToTelegram(String userId, long chatId, TelegramHolder telegramHolder) {
        try {
            String keyword = telegramHolder.getKeyword();
            String acptNm = telegramHolder.getAcptNm();
            String acptNo = telegramHolder.getAcptNo();
            String name = findCompanyName(telegramHolder.getIsuCd());
            String docNm = telegramHolder.getDocNm();
            String docUrl = telegramHolder.getDocUrl();
            String tndDt = telegramHolder.getTndDt();
            String _message = makeMessage(keyword, acptNm, getItemUrl(acptNo), name, docNm, docUrl, tndDt);
//            String key = telegramHolder.getDocNo() + "_" + telegramHolder.getIsuCd() + "_" + telegramHolder.getAcptNo();
//            SendMessage sendMessage = new SendMessage();
//            sendMessage.setChatId(chatId);
//            sendMessage.enableHtml(true);
//            sendMessage.setText(_message);
//            sendMessage.setReplyMarkup(new ReplyKeyboardRemove());
            loadBalancerCommandHelper.executeAsync(telegramHolder, tachyonNewsFlashBot.getBotToken(),chatId+"", userId, _message, asyncClient);



//            tachyonNewsFlashBot.executeAsync(sendMessage, new SentCallback<Message>() {
//                @Override
//                public void onResult(BotApiMethod<Message> botApiMethod, Message message) {
//                    log.info(key + " OK >>> " + userId + " " + chatId + " " + removeNewLine(_message));
//                }
//
//                @Override
//                public void onError(BotApiMethod<Message> botApiMethod, TelegramApiRequestException e) {
//                    log.info(key + " FAIL >>> " + userId + " " + chatId + " " + removeNewLine(_message));
//                }
//
//                @Override
//                public void onException(BotApiMethod<Message> botApiMethod, Exception e) {
//                    log.info(key + " FAIL >>> " + userId + " " + chatId + " " + removeNewLine(_message));
//                }
//            });

        } catch (Exception e) {
            log.error(e.getMessage());

        }

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
