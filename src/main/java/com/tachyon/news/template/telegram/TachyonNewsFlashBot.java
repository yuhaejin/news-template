package com.tachyon.news.template.telegram;

import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.Update;

@Slf4j
public class TachyonNewsFlashBot extends TelegramLongPollingBot {
    private TemplateMapper templateMapper;

    public TachyonNewsFlashBot() {
    }

    public TachyonNewsFlashBot(TemplateMapper templateMapper) {
        this.templateMapper = templateMapper;
    }

    @Override
    public void onUpdateReceived(Update update) {
        if (update.hasMessage()) {
            Message message = update.getMessage();
            long chatid = message.getChatId();
            if (message.hasText()) {
                String text = message.getText();
                String[] strings = StringUtils.splitByWholeSeparator(text, "_");
                if (strings.length == 2) {
                    if ("start".equalsIgnoreCase(strings[0])) {
                        String user = strings[1];
                        int count = templateMapper.findUser(user);
                        if (count > 0) {
                            log.info("update chatId " + chatid + " user=" + user);
                            templateMapper.updateChatId(user, chatid);
                        } else {
                            log.error("can't find user " + user);
                        }
                    }
                }
            }
        }
    }

    @Override
    public String getBotUsername() {
        return "tachyonnews_bot";
    }
    @Override
    public String getBotToken() {
        return "894631413:AAEWNVFIYEs7uN34Q-zlqGIK3rU3vNhtcJ4";
    }
}
