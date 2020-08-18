package com.tachyon.news.template.command;

import com.tachyon.article.Trial;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.WholeBodyParser;
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
 * 임상실험 공시 처리...
 */
@Slf4j
@Component
public class TrialHolderCommand extends BasicCommand {

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
            TableParser tableParser = new TableParser(new WholeSelector());
            String key = myContext.findInputValue(message);
            DocNoIsuCd docNoIsuCd = find(key);
            if (docNoIsuCd == null) {
                log.info("... can't parse " + key);
                return;
            }

            String docNo = docNoIsuCd.getDocNo();
            String code = docNoIsuCd.getIsuCd();
            String acptNo = docNoIsuCd.getAcptNo();
            handleTrial(docNo, code, acptNo, key, tableParser);

            log.info("done " + key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void handleTrial(String docNo, String code, String acptNo, String key, TableParser tableParser) throws Exception {

        Map<String, Object> _map = templateMapper.findKongsiHolder2(docNo, code, acptNo);
        if (_map == null) {
            log.warn("공시가 없음. " + key);
            return;
        }

        if (isOldAtCorrectedKongsi(_map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }

        String docUrl = findDocUrl(_map);

        String docNm = findDocNm(_map);
        if (Maps.hasAndKey(docNm, "임상시험", "진입") == false) {
            log.info("임상시험단계 진입ㆍ종료 공시가 아님. " + key + " " + docUrl);
            return;
        }
        String filePath = filePath(myContext.getHtmlTargetPath(), docNo, code);
        log.info("... " + key + " " + docUrl + " " + filePath);
        String html = findDocRow(filePath, docUrl, retryTemplate,loadBalancerCommandHelper);
        if (StringUtils.isEmpty(html)) {
            log.warn("공시데이터가 없음. " + key + " " + docUrl);
            return;
        }

        List<Table> tables = tableParser.parseSome(html, docUrl, new WholeBodyParser(false));

        if (tables == null || tables.size() == 0) {
            log.warn("테이블데이터가 없음.. " + docNm + " " + docUrl);
            return;
        }

        if (tables.size() > 1) {
            log.warn("분석패턴과 다름. " + key + " " + docUrl);
            return;
        }
        Map<String, Object> map = Tables.wideOneLineTableToMap(tables.get(0).getBodies(), true, "");
        Trial trial = Trial.from(map);
        log.info(trial.toString());
        if (docNm.contains("정정")) {
            String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
            if (StringUtils.isEmpty(_docNo) == false) {
                log.info("정정공시중에 이전 공시 삭제... " + _docNo + " " + code);
                templateMapper.deleteBeforeTrialHolder(code, _docNo);
            }
        }
        Map<String, Object> findParam = findParam(docNo, code, acptNo);
        if (templateMapper.findTrialCount(findParam) == 0) {
            Map<String, Object> insertParam = param(trial, docNo, code, acptNo);
            insertTrial(insertParam);
            if (isGoodArticle(docNm)) {
                sendToArticleQueue(rabbitTemplate,findPk(insertParam),"TRIAL",findParam);
            }
        } else {
            log.info("SKIP 이미 존재함. " + key);
        }

    }

    private void insertTrial(Map<String, Object> map ) {
        templateMapper.insertTrial(map);
    }

    private Map<String, Object> param(Trial trial, String docNo, String code, String acptNo) {
        Map<String, Object> map = new HashMap<>();
        map.put("doc_no", docNo);
        map.put("isu_cd", code);
        map.put("acpt_no", acptNo);
        map.put("type", trial.getType());
        map.put("title", trial.getTitle());
        map.put("purpose", trial.getPurpose());
        map.put("approval_agency", trial.getApprovalAgency());
        map.put("approval_date", modifyDate(trial.getApprovalDate()));
        map.put("approval_history", trial.getApprovalHistory());
        map.put("future_plan", trial.getFuturePlan());
        map.put("confirm_date", modifyDate(trial.getConfirmDate()));
        map.put("etc", trial.getEtc());
        map.put("acpt_dt", acptNo.substring(0, 8));

        return map;
    }

    private String modifyDate(String date) {
        StringBuilder sb = new StringBuilder();
        char[] chars = date.toCharArray();
        for (char c : chars) {
            if (Character.isDigit(c)) {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
