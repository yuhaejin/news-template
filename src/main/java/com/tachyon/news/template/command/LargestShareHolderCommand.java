package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.model.LargestStock;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.LargestStockHolderParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByElement;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 최대주주등 소유주식변동신고서..
 * 최대주주등 주식소유현황 테이블 분석
 *
 * 해당 공시파일(html)을 읽고 분석.
 *
 */
@Slf4j
@Component
public class LargestShareHolderCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Override
    public void execute(Message message) throws Exception {

        String key = myContext.findInputValue(message);
        log.info("... " + key);

        DocNoIsuCd docNoIsuCd = find(key);
        if (docNoIsuCd == null) {
            log.info("... can't parse " + key);
            return;
        }

        String code = docNoIsuCd.getIsuCd();
        String docNo = docNoIsuCd.getDocNo();
        String acptNo = docNoIsuCd.getAcptNo();

        Map<String, Object> map = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
        if (map == null) {
            log.error("기초공시가 없음.. " + key);
            return;
        }
        log.info(key+" "+map.toString());
        String docNm = Maps.getValue(map, "doc_nm");
        String docUrl = Maps.getValue(map, "doc_url");
        String tnsDt = Maps.getValue(map, "tns_dt");
        String acptNm = Maps.getValue(map, "rpt_nm");

        if (isMajorStockChangeKongis(docNm)==false) {
            log.info("대상 공시가 아님.. "+key);
            return;
        }

        if (isOldAtCorrectedKongsi(map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }

        String docRaw = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, loadBalancerCommandHelper);

        StockChangeSelectorByElement selector = new StockChangeSelectorByElement();
        selector.setKeywords(new String[]{"최대", "주주", "주식","소유","현황"});

        TableParser stockChangeTableParser = new TableParser(selector);
        LargestStockHolderParser myParser = new LargestStockHolderParser();
        myParser.setStrings(new String[]{"주식수", "비율"});

        List<Table> tables = stockChangeTableParser.parseSome(docRaw, docUrl, myParser);
        List<LargestStock> stocks = new ArrayList<>();
        if (tables != null && tables.size() > 0) {
            for (Table _table : tables) {
                List<Map<String, Object>> maps = _table.toMapList();
                for (Map<String, Object> _map : maps) {
                    LargestStock largestStock = LargestStock.from(_map,code,docNo,acptNo);
                    if (largestStock != null) {
                        if (isTotalName(largestStock.getName())) {
                            log.info("SKIP 합계.. ");
                            continue;
                        }
                        stocks.add(largestStock);
                    }
                }
            }

            if (isChangeKongsi(acptNm)) {
                if (stocks.size()>0) {
                    String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
                    log.info("이전LargestStockHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);
                    if (StringUtils.isEmpty(_docNo) == false) {
                        if (docNo.equalsIgnoreCase(_docNo) == false) {
                            deleteBeforeLargestStockHolder(templateMapper, code, _docNo);
                            log.info("이전LargestStockHolder 삭제 code=" + code + " docNo=" + _docNo);
                        }
                    }
                }
            }

        } else {
            log.warn("최대주주등 주식소유현황 테이블 없음. " +key);
        }

        if (stocks.size() > 0) {
            for (LargestStock stock : stocks) {
                Map<String, Object> param = stock.paramMap();
                if (findLargestStockHolderCount(templateMapper, param) == 0) {
                    log.info(param.toString());
                    templateMapper.insertLargestStockHolder(param);
                } else {
                    log.info("SKIP 중복된 데이터 "+param);
                }
            }
        }



        log.info("done " + key);

    }

    private boolean isTotalName(String value) {
        value = StringUtils.remove(value, " ");
        if (value.contains("합계")) {
            return true;
        } else {
            return false;
        }
    }
    private int findLargestStockHolderCount(TemplateMapper templateMapper, Map<String, Object> keyParam) {
        return templateMapper.findLargestStockHolderCount(keyParam);
    }

    private void deleteBeforeLargestStockHolder(TemplateMapper templateMapper, String code, String docNo) {
        templateMapper.deleteBeforeLargestStockHolder(code, docNo);
    }

    /**
     * TODO 정정 이전 공시 삭제처리..
     * @param code
     * @param acptNo
     */
    private void handleSrcCorrectKongsi(String code, String acptNo) {
//        for (Kongsi.UrlInfo urlInfo : urlInfos) {
//            String docNm = urlInfo.getDocNm();
//            if (docNm.contains("정정") == false) {
//                String docNo = urlInfo.getDocNo();
//                log.info("정정이전공시 삭제 " + docNo + "_" + code + "_" + acptNo);
//                templateMapper.deleteOverlapRumor(docNo, code, acptNo);
//            }
//        }
    }
}
