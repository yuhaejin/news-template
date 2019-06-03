package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Purpose;
import com.tachyon.crawl.kind.model.Purposes;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import jongsong.kim.sample.BusinessPurposeSelector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class PurposeHolderCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;

    @Autowired
    private TemplateMapper templateMapper;

    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Override
    public void execute(Message message) throws Exception {
        String key = myContext.findInputValue(message);
        log.info("<<< "+key);
        String[] keys = StringUtils.split(key, "_");
        String docNo = keys[0];
        String code = keys[1];
        String acptNo = keys[2];
        Map<String, Object> kongsiHolder = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
        if (kongsiHolder == null) {
            log.error("기초공시가 없음 docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
            return;
        }
        String docUrl = Maps.getValue(kongsiHolder, "doc_url");
        String c = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, loadBalancerCommandHelper);

        TableParser parser = new TableParser(new BusinessPurposeSelector());

        Table table = (Table) parser.parse(c , docUrl);
        if (table != null) {
            List<Map<String, Object>> maps = table.toPurposeMapList();
            Purposes purposes = new Purposes();
            purposes.from(maps);


            List<Purpose> list = purposes.findPurpose();
            log.info("사업목적변경 갯수 "+list.size()+" docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
            for (Purpose purpose : list) {
                purpose.setCode(code);
                purpose.setDocNo(docNo);
                purpose.setDocUrl(docUrl);
                purpose.setFilePath("");
                log.info("... "+purpose);
                handleDb(purpose);
            }


        } else {
            log.info("사업목적 변경 세부내역 데이터가 없음. docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
        }

        log.info("done "+key);
    }

    private void handleDb(Purpose purpose) {
        Map<String, Object> param = purpose.findParam();
        int count = templateMapper.findPurposeHolder(param);
        if (count == 0) {
            if (BizUtils.isEmpty(purpose.getContents())) {
                templateMapper.insertPurposeHolder2(purpose.insertParam());
            } else {
                templateMapper.insertPurposeHolder1(purpose.insertParam());
            }
        } else {
            log.info("이미처리됨 "+purpose.getCode()+" "+purpose.getDocNo()+" "+purpose.getDocUrl());
        }
    }
}
