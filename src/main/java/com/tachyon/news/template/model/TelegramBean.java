package com.tachyon.news.template.model;

import java.io.Serializable;

/**
 * 텔레그램으로 전종할 데이터 정보를 가지는 클래스.
 */
public class TelegramBean implements Serializable {
    private String userId;
    private long chatId;
    private TelegramHolder telegramHolder;

    public String getUserId() {
        return userId;
    }

    public TelegramHolder getTelegramHolder() {
        return telegramHolder;
    }

    public void setTelegramHolder(TelegramHolder telegramHolder) {
        this.telegramHolder = telegramHolder;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getChatId() {
        return chatId;
    }

    public void setChatId(long chatId) {
        this.chatId = chatId;
    }

    public TelegramBean(String userId, long chatId, TelegramHolder telegramHolder) {
        this.userId = userId;
        this.chatId = chatId;
        this.telegramHolder = telegramHolder;
    }


}
