package com.tachyon.news.template.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class Keyword implements Serializable {
    private String keyword;
    private int order;

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public Keyword() {
    }

    public Keyword(String keyword, int order) {
        this.keyword = keyword;
        this.order = order;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("keyword", keyword)
                .append("order", order)
                .toString();
    }
}
