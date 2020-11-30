package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.parser.DebugMyParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.FinancialStatementSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 실적 처리
 * 분기,반기,사업보고서에서 손익계산서 부분을 분석하여
 * 연결재무제표, 재무제표의
 * 수익(매출액)
 * 영업이익(손실)
 * 당기순이익(손실)
 * 데이터를 저장함.
 */
@Slf4j
@Component
public class PerformanceCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;

    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;


    @Override
    public void execute(Message message) throws Exception {
        String key = myContext.findInputValue(message);
        log.info("<<< " + key);

        String[] keys = StringUtils.split(key, "_");
        String docNo = keys[0];
        String code = keys[1];
        String acptNo = keys[2];
        Map<String, Object> kongsiHolder = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
        if (kongsiHolder == null) {
            log.error("기초공시가 없음 docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
            return;
        }

        String rptNm = Maps.getValue(kongsiHolder, "rpt_nm");
        String docUrl = Maps.getValue(kongsiHolder, "doc_url");
        if (StringUtils.containsAny(rptNm, "분기보고서", "반기보고서", "사업보고서")==false) {
            log.info("SKIP 실적관련 공시가 아님.. " + rptNm + " rptNm=" + docNo + " code=" + code + " acptNo=" + acptNo + " " + docUrl);
        }

//        FinancialStatementSelectorByPattern selector = new FinancialStatementSelectorByPattern();
//        TableParser tableParser = new TableParser(selector);
//        selector.setKeywords(new String[]{"연결", "재무", "제표"});
//        selector.setNo_keywords(new String[]{});
//        List<Map<String, Object>> maps = (List<Map<String, Object>>) tableParser.simpleParse(c, docUrl, new DebugMyParser());
    }
}
