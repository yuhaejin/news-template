package com.tachyon.news.template;

import com.tachyon.news.template.telegram.TachyonMonitoringBot;
import com.tachyon.news.template.telegram.TachyonNewsFlashBot;
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

    @Test
    public void testSending() throws TelegramApiException {
        int index = 0;

        TachyonMonitoringBot tachyonMonitoringBot = new TachyonMonitoringBot();
        TachyonNewsFlashBot tachyonNewsFlashBot = new TachyonNewsFlashBot();
        while (true) {
            long start = System.currentTimeMillis();
            if (index++ % 2 == 0) {
                sendMonitoring(tachyonMonitoringBot);
            } else {
                sendMonitoring(tachyonNewsFlashBot);
            }

            System.out.println(System.currentTimeMillis()-start);
            if (index == 10) {
                break;
            }
        }
    }

    private void sendMonitoring(TachyonMonitoringBot tachyonMonitoringBot) throws TelegramApiException {
        SendMessage sendMessage = new SendMessage();
        sendMessage.setChatId("334618638");

        sendMessage.enableHtml(false);
        sendMessage.setText("HI Hello...");
        sendMessage.setReplyMarkup(new ReplyKeyboardRemove());
        tachyonMonitoringBot.execute(sendMessage);
    }

    private void sendMonitoring(TachyonNewsFlashBot tachyonNewsFlashBot) throws TelegramApiException {
        SendMessage sendMessage = new SendMessage();
        sendMessage.setChatId("334618638");
        sendMessage.enableHtml(false);
        sendMessage.setText("HI Hello...");
        sendMessage.setReplyMarkup(new ReplyKeyboardRemove());
        tachyonNewsFlashBot.execute(sendMessage);
    }
}
