package com.tachyon.news.template.command;

import com.tachyon.article.Touch;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.EtcKongsiParser;
import com.tachyon.crawl.kind.parser.MyTouchParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.WholeSelector;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.Tables;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 기타 경영 공시 처리.
 */
@Slf4j
@Component
public class OtherManagementCommand extends BasicCommand {

    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private MyContext myContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private RetryTemplate retryTemplate;
    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Override
    public void execute(Message message) throws Exception {
        try {
            String key = myContext.findInputValue(message);
            DocNoIsuCd docNoIsuCd = find(key);
            if (docNoIsuCd == null) {
                log.info("... can't parse " + key);
                return;
            }

            String docNo = docNoIsuCd.getDocNo();
            String code = docNoIsuCd.getIsuCd();
            String acptNo = docNoIsuCd.getAcptNo();
            TableParser parser = new TableParser(new WholeSelector());

            handleOtherManagement(docNo, code, acptNo, key, parser);

            log.info("done " + key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void handleOtherManagement(String docNo, String code, String acptNo, String key, TableParser tableParser) throws Exception {

        Map<String, Object> _map = templateMapper.findKongsiHolder2(docNo, code, acptNo);
        if (_map == null) {
            log.info("공시가 없음. " + key);
            return;
        }

        if (isOldAtCorrectedKongsi(_map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }

        String docUrl = findDocUrl(_map);
        String docNm = findDocNm(_map);

        if (Maps.hasAndKey(docNm, "기타", "경영사항") == false) {
            log.info("기타 경영사항 공시가 아님. " + key + " " + docUrl);
            return;
        }
        String filePath = filePath(myContext.getHtmlTargetPath(), docNo, code);
        log.info("... " + key + " " + docUrl + " " + filePath);
        String html = findDocRow(filePath, docUrl, retryTemplate, loadBalancerCommandHelper);
        if (StringUtils.isEmpty(html)) {
            log.info("공시데이터가 없음. " + key + " " + docUrl);
            return;
        }

        EtcKongsiParser myParser = new EtcKongsiParser();
        List<Map<String, Object>> tables = (List<Map<String, Object>>) tableParser.simpleParse(html, docUrl, myParser);

        if (tables == null || tables.size() == 0) {
            log.info("테이블데이터가 없음.. " + docNm + " " + docUrl);
            return;
        }

        if (docNm.contains("정정")) {
            String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
            if (StringUtils.isEmpty(_docNo) == false) {
                log.info("정정공시중에 이전 공시 삭제... " + _docNo + " " + code);
                templateMapper.deleteBeforeTouchHolder(_docNo, code);
            }
        }

        Map<String, Object> param = param();
        // code, 타이틀 결정일자 로 구분이 가능해 보임..
        Map<String, Object> findParam = findParam(docNo, code, acptNo);
        if (templateMapper.findEtcManagementCount(findParam) == 0) {
            templateMapper.insertEtcManagement(param);
        } else {
            templateMapper.updateEtcManagement(param);
        }
    }


    private Map<String, Object> param() {
        Map<String, Object> map = new HashMap<>();


        return map;
    }
}
