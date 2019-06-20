package com.tachyon.news.template.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class Bot implements Serializable {
    private String seq;
    private String name;
    private String token;

    public Bot(String seq, String name, String token) {
        this.seq = seq;
        this.name = name;
        this.token = token;
    }

    public String getSeq() {
        return seq;
    }

    public void setSeq(String seq) {
        this.seq = seq;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("seq", seq)
                .append("name", name)
                .append("token", token)
                .toString();
    }
}
