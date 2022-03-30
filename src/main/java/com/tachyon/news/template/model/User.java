package com.tachyon.news.template.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class User implements Serializable {
    private String userid;
    private List<Keyword> keywords;
    private List<String> codes = new ArrayList<>();
    // grade가 낮은 사용자부터 처리함. grade 99이면 모든 종목 처리함.
    private int grade;
    private long chatId;
    private long botSeq;
    private Bot bot;

    public Bot getBot() {
        return bot;
    }

    public void setBot(Bot bot) {
        this.bot = bot;
    }

    public long getBotSeq() {
        return botSeq;
    }

    public void setBotSeq(long botSeq) {
        this.botSeq = botSeq;
    }

    public long getChatId() {
        return chatId;
    }

    public void setChatId(long chatId) {
        this.chatId = chatId;
    }

    public void addCode(String code) {
        codes.add(code);
    }

    public List<String> getCodes() {
        return codes;
    }
    public int getGrade() {
        return grade;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public List<Keyword> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<Keyword> keywords) {
        this.keywords = keywords;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("userid", userid)
                .append("keywords", keywords)
                .append("codes", codes)
                .append("grade", grade)
                .append("chatId", chatId)
                .append("botSeq", botSeq)
                .append("bot", bot)
                .toString();
    }

    public boolean hasKeyword(String _keyword) {
        for (Keyword keyword : keywords) {
            if (_keyword.equalsIgnoreCase(keyword.getKeyword())) {
                return true;
            }
        }
        return false;
    }

    public boolean hasCode(String isuCd) {
        if (grade == 99) {
            return true;
        } else {
            if (codes.contains(isuCd)) {
                return true;
            } else {
                return false;
            }
        }
    }
}
