package com.tachyon.news.template.command;

import com.tachyon.article.Touch;
import com.tachyon.crawl.kind.model.Table;
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
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class TouchCommand extends BasicCommand {

    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private MyContext myContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;

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
            handleTouch(null, docNo, code, acptNo, key, tableParser);

            log.info("done " + key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void handleTouch(Map<String, Object> _map, String docNo, String code, String acptNo, String key, TableParser tableParser) throws Exception {

        if (_map == null) {
            _map = templateMapper.findKongsiHalder2(docNo, code, acptNo);
            if (_map == null) {
                log.info("공시가 없음. " + key);
                return;
            }
        }

        if (isOldAtCorrectedKongsi(_map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }

        String docUrl = findDocUrl(_map);

        String docNm = findDocNm(_map);
        if (Maps.hasAndKey(docNm, "매출액", "손익") == false) {
            log.info("매출액 손익 공시가 아님. " + key + " " + docUrl);
            return;
        }
        String filePath = filePath(myContext.getHtmlTargetPath(), docNo, code);
        log.info("... " + key + " " + docUrl + " " + filePath);
        String html = findDocRow(filePath, docUrl, loadBalancerCommandHelper);
        if (StringUtils.isEmpty(html)) {
            log.info("공시데이터가 없음. " + key + " " + docUrl);
            return;
        }

        String rptNm = findRptNm(_map);
        List<Table> tables = tableParser.parseSome(html, docUrl, new MyTouchParser(rptNm));

        if (tables == null || tables.size() == 0) {
            log.info("테이블데이터가 없음.. " + docNm + " " + docUrl);
            return;
        }

        Map<String, Object> map = null;
        if (tables.size() == 1) {
            map = Tables.wideTitleValueTable(tables.get(0).getBodies());
        } else {
            map = Tables.wideTitleValueTable(tables.get(0), tables.subList(1, tables.size()).toArray(new Table[0]));
        }

        log.info(map.size() + " " + map);
        if (map.size() <= 10) {
            log.info("처리할 수 있는 테이블이 아님. " + map.size() + " " + docUrl);
            return;
        }
        Touch touch = Touch.fromKongsi(map);

        if (docNm.contains("정정")) {
            String _docNo = findBeforeKongsi(templateMapper,code, acptNo);
            if (StringUtils.isEmpty(_docNo) == false) {
                log.info("정정공시중에 이전 공시 삭제... " + _docNo + " " + code);
                templateMapper.deleteBeforeTouchHolder(_docNo, code);
            }
        }

        Map<String, Object> param = param(touch, docNo, code, acptNo);
        if (templateMapper.findTouchHolderCount(docNo, code, acptNo) == 0) {
            templateMapper.insertTouchHolder(param);
        } else {
            log.info("이미 존재.. SKIP.. " + touch);
        }

    }
    private Map<String, Object> param(Touch touch, String docNo, String code, String acptNo) {
        Map<String, Object> map = new HashMap<>();
        map.put("financial_type", touch.getFinancialType());
        map.put("now_revenue", touch.getNowRevenue());
        map.put("before_revenue", touch.getBeforeRevenue());
        map.put("revenue_amount", touch.getRevenueAmount());
        map.put("revenue_ratio", touch.getRevenueRatio());
        map.put("now_profit", touch.getNowProfit());
        map.put("before_profit", touch.getBeforeProfit());
        map.put("profi_amount", touch.getProfiAmount());
        map.put("profit_ratio", touch.getProfitRatio());
        map.put("now_ordi_profit", touch.getNowOrdiProfit());
        map.put("before_ordi_profit", touch.getBeforeOrdiProfit());
        map.put("ordi_profit_amount", touch.getOrdiProfitAmount());
        map.put("ordi_profit_ratio", touch.getOrdiProfitRatio());
        map.put("now_income", touch.getNowIncome());
        map.put("before_income", touch.getBeforeIncome());
        map.put("income_amount", touch.getIncomeAmount());
        map.put("income_ratio", touch.getIncomeRatio());
        map.put("is_large_corp", touch.getIsLargeCorp());
        map.put("now_total_assets", touch.getNowTotalAssets());
        map.put("before_total_assets", touch.getBeforeTotalAssets());
        map.put("now_total_debt", touch.getNowTotalDebt());
        map.put("before_total_debt", touch.getBeforeTotalDebt());
        map.put("now_total_cap", touch.getNowTotalCap());
        map.put("before_total_cap", touch.getBeforeTotalCap());
        map.put("now_cap", touch.getNowCap());
        map.put("before_cap", touch.getBeforeCap());
        map.put("now_asscap_ratio", touch.getNowAsscapRatio());
        map.put("before_asscap_ratio", touch.getBeforeAsscapRatio());
        map.put("sales_variation_cause", StringUtils.abbreviate(touch.getSalesVariationCause(),500));
        map.put("etc", StringUtils.abbreviate(touch.getEtc(),500));
        map.put("doc_no", docNo);
        map.put("isu_cd", code);
        map.put("acpt_no", acptNo);
        map.put("acpt_dt", acptNo.substring(0,8));

        return map;
    }
}
