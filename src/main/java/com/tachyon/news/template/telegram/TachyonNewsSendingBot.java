package com.tachyon.news.template.telegram;

import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.telegram.telegrambots.bots.DefaultBotOptions;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.objects.Update;

@Slf4j
public class TachyonNewsSendingBot extends TelegramLongPollingBot {

    public TachyonNewsSendingBot(DefaultBotOptions botOptions){
        super(botOptions);
    }

    @Override
    public void onUpdateReceived(Update update) {

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
