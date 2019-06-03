package com.tachyon.news.camel.config;

import com.tachyon.news.camel.NewsConstants;
import org.apache.camel.Exchange;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class MyContext {
    @Value("${kongsi.json.targetPath}")
    private String jsonTargetPath;
    @Value("${kongsi.html.targetPath}")
    private String htmlTargetPath;
    @Value("${kongsi.template.queues}")
    private String templaeQueues;
    @Value("${kongsi.elastic-index:tachyon-news}")
    private String elasticIndex;

    @Value("${kongsi.elastic-type:kongsi}")
    private String elasticType;
    @Value("${kongsi.elastic-port:9000}")
    private int elasticPort;
    @Value("${kongsi.elastic-url:localhost}")
    private String elasticUrl;

    @Value("${kongsi.elastic-bulk-action:1000}")
    private int bulkActionCount;
    @Value("${kongsi.elastic-flush-interval:60}")
    private long flushInterval;
    @Value("${kongsi.elastic-bulk-size:20}")
    private long bulkSize;
    private String[] templates;
    private AtomicLong gatewayCount = new AtomicLong();

    public void addGatewayCalling() {
        gatewayCount.incrementAndGet();
    }

    public long getGatewayCalling() {
        return gatewayCount.get();
    }
    @PostConstruct
    public void init() {
        if (isEmpty(templaeQueues)) {
            templates = new String[0];
        } else {
            templates = StringUtils.split(templaeQueues, ",");
        }
    }



    public int getBulkActionCount() {
        return bulkActionCount;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public long getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(long bulkSize) {
        this.bulkSize = bulkSize;
    }

    private boolean isEmpty(String value) {
        if (value == null || "" .equalsIgnoreCase(value)) {
            return true;
        } else {
            return false;
        }
    }
    public String findFromRabbitMq(String queueName) {
//        return "rabbitmq:" + queueName;
        return "rabbitmq:" + queueName + "?queue=" + queueName + "&routingKey=" + queueName+"&autoDelete=false&declare=true";
    }


    public String getJsonTargetPath() {
        return jsonTargetPath;
    }

    public String getHtmlTargetPath() {
        return htmlTargetPath;
    }

    public String getTemplaeQueues() {
        return templaeQueues;
    }

    public String getElasticIndex() {
        return elasticIndex;
    }

    public String getElasticType() {
        return elasticType;
    }

    public int getElasticPort() {
        return elasticPort;
    }

    public String getElasticUrl() {
        return elasticUrl;
    }

    public String[] getTemplates() {
        return templates;
    }

    public String findInputValue(Exchange exchange) {
        byte[] bytes= (byte[])exchange.getIn().getBody();
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public String findRoutingKey(Exchange exchange) {
        return (String)exchange.getIn().getHeader("rabbitmq.ROUTING_KEY");
    }
//    public NewsBean findNewsBean(Exchange exchange) {
//        return (NewsBean)exchange.getIn().getHeader("NEWS_BEAN");
//    }
    public String findKongsiMapKey(Exchange exchange) {
        return (String)exchange.getIn().getHeader(NewsConstants.KONGSI_KEY);
    }

    public Map<String, Object> findKongsiMap(Exchange exchange) {
        String key = findKongsiMapKey(exchange);
        if (isEmpty(key)) {

            return null;
        } else {
            Map<String, Object> kongsi = new HashMap<>();
            String[] strings = StringUtils.split(key, ",");
            Map<String, Object> map = exchange.getIn().getHeaders();
            for (String _key : strings) {
                kongsi.put(_key, map.get(_key));
            }

            return kongsi;
        }
    }

//    public boolean findMetaKongsi(Exchange exchange) {
//        return (boolean)exchange.getIn().getHeader("META_KONGSI");
//    }
//
//    public boolean findKongsiFile(Exchange exchange) {
//        return (boolean)exchange.getIn().getHeader("KONGSI_FILE");
//    }
}
