package com.tachyon.news.camel.model;

import java.io.Serializable;
import java.util.Map;

public class NewsBean implements Serializable {
    private Map<String,Object> kongsi;

    public NewsBean(Map<String, Object> kongsi) {
        this.kongsi = kongsi;
    }

    public Map<String, Object> getKongsi() {
        return kongsi;
    }

    public void setKongsi(Map<String, Object> kongsi) {
        this.kongsi = kongsi;
    }
}
