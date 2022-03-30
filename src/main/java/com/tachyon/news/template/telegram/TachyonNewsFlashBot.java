package com.tachyon.news.template.telegram;

import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.ReplyKeyboardRemove;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

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
        log.info("<<< "+update);
        if (update.hasMessage()) {
            Message message = update.getMessage();
            long chatid = message.getChatId();
            if (message.hasText()) {
                String text = message.getText();
                log.info(chatid + " <<< " + text);
                String[] strings = StringUtils.splitByWholeSeparator(text, "_");
                if (strings.length == 2) {
                    if ("start".equalsIgnoreCase(strings[0].trim())) {
                        String user = strings[1].trim();
                        int count = templateMapper.findUser(user);
                        String _message = null;
                        if (count > 0) {
                            _message = "CHAT_ID=" + chatid + " for " + user;
                            templateMapper.updateChatId(user,chatid);
                        } else {
                            _message = chatid + " 등록되지 않은 사용자 " + user;
                        }

                        try {
                            SendMessage sendMessage = new SendMessage();
                            sendMessage.setChatId(chatid);
                            sendMessage.enableHtml(false);
                            sendMessage.setText(_message);
                            sendMessage.setReplyMarkup(new ReplyKeyboardRemove());
                            execute(sendMessage);
                            log.info(_message);
                        } catch (TelegramApiException e) {
                            log.error(e.getMessage() + " ... " + _message);
                        }


                    } else {
                        log.info(chatid + " start_ 로 시작하지 않은 문자..   " + text);
                    }
                } else {
                    log.info(chatid + "의미없는 문자..  " + text);
                }
            } else {

            }
        } else {
            log.info("<<< "+update);
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
