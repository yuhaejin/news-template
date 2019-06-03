package com.tachyon.news.template.service;

import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.NewsConstants;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * 공시 파일과 텍스트 파일 처리
 */
@Slf4j
@Service
public class ToolsService extends AbstractService {
    @Autowired
    private MyContext myContext;

    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Autowired
    private RabbitTemplate rabbitTemplate;


    public ToolsService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    public void consume(Message message) {
        try {
            infoView(message,log);
            String key = myContext.findInputValue(message);
            log.info("<<< " + key);
            String[] keys = StringUtils.split(key, "_");
            String docNo = keys[0];
            String code = keys[1];
            String acptNo = keys[2];
            Map<String,Object> kongsiMap = findKongsiHalder2(docNo, code, acptNo);

            if (kongsiMap != null) {
                String docUrl = Maps.getValue(kongsiMap, "doc_url");
                String content = loadBalancerCommandHelper.findBody(docUrl);
                log.info(content.length()+" <<< "+StringUtils.abbreviate(content, 100));

            }
        } catch (Exception e) {
            handleError(rabbitTemplate, message, e, log);
        }
    }


    public void error(Message message) {

        String value = findKey(message);
        String fromQueue = findHeader(message,NewsConstants.FROM_QUEUE);
        String errmsg = findHeader(message,NewsConstants.ERR_MSG);

        log.info(fromQueue+" ===> "+ value+" "+errmsg);

        saveError(fromQueue, value,errmsg);
    }


}
