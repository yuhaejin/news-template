package com.tachyon.news.template.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class CorrectBean implements Serializable {
    private String type;
    private int index;

    public CorrectBean(String type, int index) {
        this.type = type;
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .append("index", index)
                .toString();
    }
}
