package com.tachyon.news.template;

import com.tachyon.news.template.telegram.TachyonMonitoringBot;
import org.junit.Test;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.ReplyKeyboardRemove;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

public class MonitoringBotTest {
    @Test
    public void testMonitoring() throws TelegramApiException {
        TachyonMonitoringBot tachyonMonitoringBot = new TachyonMonitoringBot();
        SendMessage sendMessage = new SendMessage();
        sendMessage.setChatId("334618638");
        sendMessage.enableHtml(false);
        sendMessage.setText("HI Hello...");
        sendMessage.setReplyMarkup(new ReplyKeyboardRemove());
        tachyonMonitoringBot.execute(sendMessage);
    }
}
