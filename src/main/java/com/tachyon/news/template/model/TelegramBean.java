package com.tachyon.news.template.model;

import java.io.Serializable;

/**
 * 텔레그램으로 전종할 데이터 정보를 가지는 클래스.
 */
public class TelegramBean implements Serializable {
    private String userId;
    private int chatId;
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

    public int getChatId() {
        return chatId;
    }

    public void setChatId(int chatId) {
        this.chatId = chatId;
    }

    public TelegramBean(String userId, int chatId, TelegramHolder telegramHolder) {
        this.userId = userId;
        this.chatId = chatId;
        this.telegramHolder = telegramHolder;
    }


}
