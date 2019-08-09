package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.model.Rumor;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.MajorStockChangeParser;
import com.tachyon.crawl.kind.parser.NoHeaderMyParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.RumorSelector;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByPattern2;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 최대주주 공시 처리..
 * docNo_code_acptNo 가 키
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
        String docNm = Maps.getValue(map, "doc_nm");
        String docUrl = Maps.getValue(map, "doc_url");
        String tnsDt = Maps.getValue(map, "tns_dt");
        String acptNm = Maps.getValue(map, "rpt_nm");

        if (isMajorStockChangeKongis(docNm)==false) {
            log.info("대상 공시가 아님.. "+key);
            return;
        }
        String docRaw = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, loadBalancerCommandHelper);

        StockChangeSelectorByPattern2 pattern2 = new StockChangeSelectorByPattern2();
        pattern2.setKeywords(new String[]{"최대", "주주", "주식","소유","현황"});

        /**
         * TITLE을 보존할 수 있도록 수정해야 함.
         */
        TableParser stockChangeTableParser = new TableParser(pattern2);
        MajorStockChangeParser myParser = new MajorStockChangeParser();
        myParser.setStrings(new String[]{"주식수", "비율"});

        List<Table> tables = stockChangeTableParser.parseSome(docRaw, docUrl, myParser);
        if (tables != null && tables.size() > 0) {
            for (Table _table : tables) {
                List<Map<String, Object>> maps = _table.toMapList();
                for (Map<String, Object> _map : maps) {
                    for (String _key : _map.keySet()) {
                        log.info(_key+" ::: "+_map.get(_key));
                    }
                }
            }
        } else {
            log.info("최대주주등 주식소유현황 테이블 없음. ");
        }


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
