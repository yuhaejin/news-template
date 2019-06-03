package com.tachyon.news.template.config;

import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.UpdatesListener;
import com.pengrad.telegrambot.model.Message;
import com.pengrad.telegrambot.model.Update;
import com.pengrad.telegrambot.request.GetUpdates;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Slf4j
@Component
public class TelegramListener {
    @Autowired
    private TelegramBot telegramBot;
    @Autowired
    private TemplateMapper templateMapper;
    @PostConstruct
    public void init() {
        telegramBot.setUpdatesListener(new UpdatesListener() {
            @Override
            public int process(List<Update> list) {
                for (Update update : list) {
                    Message message = update.message();
                    long chatid = message.chat().id();
                    String text = message.text().trim();
                    log.info("<<< "+chatid+" "+message.text());
                    String[] strings = StringUtils.split(text, "_");
                    if (strings.length == 2) {
                        String user = strings[0].trim();
                        String s = strings[1].trim();
                        if ("START".equalsIgnoreCase(s)) {
                            int count = templateMapper.findUser(user);
                            if (count > 0) {
                                templateMapper.updateChatId(user, chatid);
                                log.info("update chatId "+chatid +" user="+user);
                            } else {
                                log.info("can't find user "+user);
                            }
                        } else {
                            log.info("SkIP "+text+" "+chatid);
                        }

                    } else {
                        log.info("SkIP "+text+" "+chatid);
                    }

                }

                return UpdatesListener.CONFIRMED_UPDATES_ALL;
            }
        },new GetUpdates());
    }
}
