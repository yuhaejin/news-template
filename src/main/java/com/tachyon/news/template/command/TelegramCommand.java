package com.tachyon.news.template.command;

import com.pengrad.telegrambot.Callback;
import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.model.request.ParseMode;
import com.pengrad.telegrambot.model.request.ReplyKeyboardRemove;
import com.pengrad.telegrambot.request.SendMessage;
import com.pengrad.telegrambot.response.SendResponse;
import com.tachyon.news.template.config.MyContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class TelegramCommand extends BasicCommand {


    @Autowired
    private TelegramBot telegramBot;

    @Autowired
    private MyContext myContext;

    /**
     * docNo_isuCd
     *
     * @param message
     * @throws Exception
     */
    @Override
    public void execute(Message message) throws Exception {

        String key = myContext.findInputValue(message);
        log.info(key+" <<<");

        int chatId = findHeaderAsInt(message, "chatId");
        if (chatId == 0) {
            log.error("chatId를 찾을 수 없음. "+key);
            return;
        }
        String _message = findHeader(message, "message");
        if (isEmpty(_message)) {
            log.error("텔레그램으로 전송할 메세지가 없음. "+key);
            return;
        }

        String userId = findHeader(message, "userId");
        if (isEmpty(userId)) {
            log.error("userId를 찾을 수 없음. "+key);
            return;
        }
        SendMessage request = new SendMessage(chatId, _message)
                .parseMode(ParseMode.HTML)
                .disableWebPagePreview(true)
                .disableNotification(false)
                .replyMarkup(new ReplyKeyboardRemove());

        telegramBot.execute(request, new Callback<SendMessage, SendResponse>() {
            @Override
            public void onResponse(SendMessage request, SendResponse response) {
                log.info("OK >>> "+userId+" "+chatId+" " + removeNewLine(_message));
            }

            @Override
            public void onFailure(SendMessage request, IOException e) {
                log.info("FAIL >> "+userId +" "+chatId+" "+ removeNewLine(_message));
            }
        });
    }
    private String removeNewLine(String value) {
        return StringUtils.remove(value, "\n");
    }

}
