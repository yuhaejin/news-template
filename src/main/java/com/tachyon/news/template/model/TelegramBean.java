package com.tachyon.news.template.model;

import com.tachyon.crawl.kind.model.TelegramHolder;

import java.io.Serializable;

/**
 * 텔레그램으로 전종할 데이터 정보를 가지는 클래스.
 */
public class TelegramBean implements Serializable {
    private User user;
    private TelegramHolder telegramHolder;

    public TelegramBean(User user, TelegramHolder telegramHolder) {
        this.user = user;
        this.telegramHolder = telegramHolder;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }


    public TelegramHolder getTelegramHolder() {
        return telegramHolder;
    }

    public void setTelegramHolder(TelegramHolder telegramHolder) {
        this.telegramHolder = telegramHolder;
    }


}
