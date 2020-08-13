package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.model.Kongsi;
import com.tachyon.crawl.kind.model.Rumor;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.NoHeaderMyParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.RumorSelector;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 풍문 공시 처리..
 * docNo_code_acptNo 가 키
 * 해당 공시파일(html)을 읽고 분석.
 */
@Slf4j
@Component
public class RumorHolderCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;
    @Autowired
    private RetryTemplate retryTemplate;
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

        if (isOldAtCorrectedKongsi(map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }

        String docNm = Maps.getValue(map, "doc_nm");
        String docUrl = Maps.getValue(map, "doc_url");
        String tnsDt = Maps.getValue(map, "tns_dt");
        String acptNm = Maps.getValue(map, "rpt_nm");

        // DB 확인...
        int count = templateMapper.findRumorCount(docNo, code, acptNo);
        if (count > 0) {
            log.info("SkIP :  이미처리됨. " +  key + " " + docUrl);
            return;
        }

        if ((docNm.contains("풍문") && docNm.contains("보도")) == false) {
            log.info("SkIP :  공시명 " + docNm + " " + key + " " + docUrl);
            return;
        }

        String html = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl,retryTemplate, loadBalancerCommandHelper);
        if (isEmpty(html)) {
            log.error("공시Html을 수집할 수 없음. " + key + " " + docUrl);
            return;
        }

        TableParser parser = new TableParser(new RumorSelector());
        NoHeaderMyParser myParser = new NoHeaderMyParser();
        List<Table> tables = parser.parseSome(html, docUrl, myParser);
        List<Rumor> rumors = new ArrayList<>();
        for (Table table : tables) {
            table.findRumor(code, docNo, docUrl, docNm, acptNo, rumors);
        }

        if (rumors.size() == 0) {
            log.info("풍문데이터가 없음.  " + key + " " + docUrl);
            return;
        }

        handleSrcCorrectKongsi(code,acptNo);
        for (Rumor rumor : rumors) {
            log.info(rumor.toString() +" "+ key + " " + docUrl);
            if (rumor.getSkips().size() > 0) {
                for (String skip : rumor.getSkips()) {
                    log.warn("SKIP 데이터가 존재함. " + skip+" "+key+" "+docUrl);
                }
            }
            // 최종적으로 날짜관련 데이터 설정하게 함.
            rumor.setupDt(tnsDt);
            templateMapper.insertRumor(rumor.toParamMap());
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
