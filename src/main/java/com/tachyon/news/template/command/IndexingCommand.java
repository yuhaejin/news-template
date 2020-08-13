package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.NewsConstants;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class IndexingCommand extends BasicCommand {


    @Autowired(required = false)
    private BulkProcessor bulkProcessor;
    @Autowired(required = false)
    private RestHighLevelClient restHighLevelClient;
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private RetryTemplate retryTemplate;
    @Autowired
    private MyContext myContext;

    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    /**
     * docNo_isuCd
     *
     * @param message
     * @throws Exception
     */
    @Override
    public void execute(Message message) throws Exception {

        String key = myContext.findInputValue(message);
        log.info("... " + key);

        boolean reindexing = doReIndexing(message,key);

        key = modifyKey(key);

        String[] keys = StringUtils.split(key, "_");
        String docNo = keys[0];
        String code = keys[1];
        String acptNo = keys[2];

        Map<String, Object> map = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
        if (map == null) {
            log.error("기초공시가 없어 Index 불가. docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
            return;
        }

        String name = templateMapper.findName(code);
        if (findKongsiIndex(docNo) == false) {
            IndexRequest indexRequest = fileIndex(docNo, code, acptNo, name, map);
            if (indexRequest != null) {
                bulkProcessor.add(indexRequest);
            }
        } else {
            if (reindexing == true) {
                IndexRequest indexRequest = fileIndex(docNo, code, acptNo, name, map);
                if (indexRequest != null) {
                    bulkProcessor.add(indexRequest);
                }
            } else {

                log.info("이미 인덱스됨.. docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
            }
        }

    }

    private boolean doReIndexing(Message message,String key) {
        if (message.getMessageProperties().getHeaders().containsKey(NewsConstants.REINDEXING)) {
            return true;
        } else {

            if (key.contains(NewsConstants.REINDEX)) {
                return true;
            } else {
                return false;
            }
        }
    }

    private UpdateRequest findUpdate(String docNo, String code, String name, Map<String, Object> map) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("isu_nm", name);
        log.info("... addUpdateIndexing http://" + myContext.getElasticUrl() + ":" + myContext.getElasticPort() + "/tachyon-news/kongsi/" + docNo);
        return new UpdateRequest(
                "tachyon-news",
                "kongsi",
                docNo).doc(jsonMap);

    }

    private String modifyKey(String key) {
        return StringUtils.remove(key, NewsConstants.REINDEX);
    }

    private boolean findKongsiIndex(String docNo) throws IOException {
        GetRequest getRequest = new GetRequest(
                myContext.getElasticIndex(),
                myContext.getElasticType(),
                docNo);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        return restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);

    }

    private IndexRequest fileIndex(String docNo, String code, String acptNo, String name, Map<String, Object> map) throws IOException {
        String docUrl = Maps.getValue(map, "doc_url");
        String docRaw = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl,retryTemplate, loadBalancerCommandHelper);
        if (isEmpty(docRaw)) {
            log.error("공시의 내용이 확인할 수 없음.. docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
            return null;
        }
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.putAll(map);

        jsonMap.put("contents", docRaw);
        jsonMap.put("isu_nm", name);
        modifyTnsDt(jsonMap);

        log.info("... addIndexing http://" + myContext.getElasticUrl() + ":" + myContext.getElasticPort() + "/" + myContext.getElasticIndex() + "/" + myContext.getElasticType() + "/" + docNo);
        return new IndexRequest(
                myContext.getElasticIndex(),
                myContext.getElasticType(),
                docNo).source(jsonMap);

    }

    private void modifyTnsDt(Map<String, Object> map) {
        String tndDt = (String) map.get("tns_dt");
        String acptNo = (String) map.get("acpt_no");
        if (tndDt.length() == 4) {
            map.put("tns_dt", acptNo.substring(0, 8) + tndDt);
        }
    }
}
