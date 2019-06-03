package com.tachyon.news.template;

import com.tachyon.crawl.kind.model.DocNoIsuCd;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.amqp.core.Message;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public class BaseObject {

    protected DocNoIsuCd findDocNoIsuCd(String key) {
        String[] keys = StringUtils.split(key, "_");
        String docNo = keys[0];
        String code = keys[1];
        String acptNo = keys[2];

        return new DocNoIsuCd(docNo, code, acptNo);
    }

    protected void infoView(Message exchange, Logger log) {
        viewByInfo(exchange, log);
//        viewByDebug(exchange, log);
    }

    private void viewByInfo(Message message, Logger log) {
        Map<String,Object> properties = message.getMessageProperties().getHeaders();
        log.info("message properties ...");
        infoView(properties,log);
    }

    private void viewByDebug(Message message, Logger log) {
        Map<String,Object> properties =  message.getMessageProperties().getHeaders();
        log.debug("message properties ...");
        debugView(properties,log);
    }
    protected void debugView(Map<String, Object> map,Logger log) {
        for (String key : map.keySet()) {
            log.debug(key + " " + map.get(key));
        }
    }
    protected void infoView(Map<String, Object> map,Logger log) {
        for (String key : map.keySet()) {
            log.info(key + " " + map.get(key));
        }
    }
    protected Collection<File> listFiles(File dir) {
        return FileUtils.listFiles(dir, FileFilterUtils.fileFileFilter(), FileFilterUtils.falseFileFilter());
    }


    protected String findHeader(Message message,String key) {
        Map<String, Object> headers = message.getMessageProperties().getHeaders();
        if (headers.containsKey(key)) {
            return (String) headers.get(key);
        } else {
            return "";
        }
    }
    protected Integer findHeaderAsInt(Message message,String key) {
        Map<String, Object> headers = message.getMessageProperties().getHeaders();
        if (headers.containsKey(key)) {
            return (Integer) headers.get(key);
        } else {
            return 0;
        }
    }
}
