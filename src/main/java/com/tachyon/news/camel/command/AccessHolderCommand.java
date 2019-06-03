package com.tachyon.news.camel.command;

import com.tachyon.crawl.kind.model.Access;
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.news.camel.config.MyContext;
import com.tachyon.news.camel.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 사용자가 접근하는 정보를 저장함.
 * 어느 경로로 어떤 파라미터로 조회하는지....
 *
 */
@Slf4j
@Component
public class AccessHolderCommand extends BasicCommand {
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private MyContext myContext;
    @Override
    public void execute(Exchange exchange) throws Exception {

        String key = myContext.findInputValue(exchange);
        log.info("... " + key);
        Access access = JsonUtils.toObject(key, Access.class);
        log.info(" "+access);
        templateMapper.insertAccessHolder(access.toParamMap());

    }
}
