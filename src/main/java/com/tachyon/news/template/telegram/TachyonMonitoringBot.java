package com.tachyon.news.template.telegram;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.Update;

@Slf4j
public class TachyonMonitoringBot extends TelegramLongPollingBot {

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
                        log.info("START chatId " + chatid + " user=" + user);
                    }
                }
            }
        }
    }

    @Override
    public String getBotUsername() {
        return "tachyon_monitor_bot";
    }

    @Override
    public String getBotToken() {
        return "895295829:AAFU1e2i9rdYF9cYMWVCyaYxHS1MMIR2l5c";
    }
}
