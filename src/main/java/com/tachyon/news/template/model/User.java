package com.tachyon.news.template.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class User implements Serializable {
    private String userid;
    private List<Keyword> keywords;
    private List<String> codes = new ArrayList<>();
    private int grade;
    private int chatId;

    public int getChatId() {
        return chatId;
    }

    public void setChatId(int chatId) {
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
