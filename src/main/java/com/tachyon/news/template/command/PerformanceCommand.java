package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.KrxCrawlerHelper;
import com.tachyon.crawl.kind.model.Performance;
import com.tachyon.crawl.kind.parser.MyParser;
import com.tachyon.crawl.kind.parser.PerformanceMyParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.FinancialStatementSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
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
        try {
            String key = myContext.findInputValue(message);
            log.info("<<< " + key);

            String[] keys = StringUtils.split(key, "_");
            String docNo = keys[0];
            String code = keys[1];
            String acptNo = keys[2];

            Map<String, Object> _map = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
            if (_map == null) {
                log.error("기초공시가 없음 docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
                return;
            }

            String isuNm = Maps.getValue(_map, "isu_nm");
            if (isKospi200(code) == false) {
                log.info("코스피200 종목이 아님.. " + code + " " + isuNm);
                return;
            }

            String rptNm = Maps.getValue(_map, "rpt_nm");
            String docNm = Maps.getValue(_map, "doc_nm");
            String docUrl = Maps.getValue(_map, "doc_url");
            if (isPerformanceKongsi(docNm) == false) {
                log.info("SKIP 실적관련 공시가 아님.. " + rptNm + " rptNm=" + docNo + " code=" + code + " acptNo=" + acptNo + " " + docUrl);
                return;
            }
            if (isOldAtCorrectedKongsi(_map)) {
                log.info("SKIP 정정공시중에 이전공시임. .. " + _map);
                return;
            }

            String path = findPath(myContext.getHtmlTargetPath(), code, docNo, "htm");
            File f = new File(path);
            if (f.exists() == false) {
                log.error("파일이 없음. " + path + " " + key);
                return;
            }
            String c = FileUtils.readFileToString(f, "UTF-8");
            c = c.trim();
            if (c.endsWith("</HTML>") == false) {
                log.error("INVALID Html 파일. " + path + " " + key);
                return;
            }

            FinancialStatementSelectorByPattern selector = new FinancialStatementSelectorByPattern();
            PerformanceMyParser myParser = new PerformanceMyParser();
            TableParser tableParser = new TableParser(selector);

            // 연결재무제표 분석..
            Map<String, Object> map = parseCfs(selector, tableParser, c, docUrl, myParser);
            Map<String, Object> map2 = parseFs(selector, tableParser, c, docUrl, myParser);
            if (map == null && map2 == null) {
                log.error("연결재무제표, 재무제표 모두 없음. " + docUrl + " " + key);
            }

            Map<String, Object> params = findParams(map, map2, acptNo, docNm, key);
            params.put("doc_no", docNo);
            params.put("isu_cd", code);
            params.put("acpt_no", acptNo);
            params.put("acpt_dt", acptNo.substring(0, 8));

            setupPeriod(params, key);
            // 정정공시 처리
            if (docNm.contains("정정")) {
                String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
                if (StringUtils.isEmpty(_docNo) == false) {
                    log.info("정정공시중에 이전 공시 삭제... " + _docNo + " " + code);
                    templateMapper.deleteBeforeBizPerfHolder(_docNo, code);
                    deleteBeforeArticle(templateMapper, _docNo, acptNo, code);
                }
            }


            if (templateMapper.findBizPerfHolderCount(params) == 0) {
                templateMapper.insertBizPerfHolder(params);
                log.info("INSERT 실적 " + params);
                if (isGoodArticle(docNm)) {
//                sendToArticleQueue(rabbitTemplate,findPk(),"BIZ_PERF",findSubPk());
                }
            } else {
                log.info("이미 존재.. SKIP.. " + params);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

    }

    private boolean isKospi200(String code) {
        return myContext.isKospi200(code);
    }

    /**
     * 기간데이터를 없으면 기본데이터로 설정함.
     *
     * @param params
     * @param key
     */
    private void setupPeriod(Map<String, Object> params, String key) {
        String period = Maps.getValue(params, "period");
        if (isEmpty(period)) {
            params.put("period", "ERROR_NOTABLE_" + key);
        } else {
            if (period.contains("ERROR")) {
                params.put("period", "ERROR_PARSING_" + key);
            }
        }
    }

    private Map<String, Object> findSubPk() {
        return null;
    }

    private String findPk() {
        return null;
    }

    private Map<String, Object> findParams(Map<String, Object> map, Map<String, Object> map2, String acptNo, String docNm, String key) {
        Map<String, Object> params = new HashMap<>();
        // 두개의 재무제표를 모두 분석하지 못함..
        if (map == null && map2 == null) {
            return params;
        }

        if (map != null) {
            Performance performance = BizUtils.findPerformance(map);
            performance.setFromData("연결재무제표");
            performance.setDocNm(docNm);
            performance.setKey(key);
            performance.setAcptUrl(KrxCrawlerHelper.getItemUrl(acptNo));
            performance.param(params);
            if (hasThreeMonthInKey(performance)) {
                performance.setParsedPeriod(BizUtils.findPeriod(performance.getPeriod(), "분기보고서"));
            } else {
                performance.setParsedPeriod(BizUtils.findPeriod(performance.getPeriod(), docNm));
            }
            log.info("연결재무제표실적 " + performance);
        }

        if (map2 != null) {
            Performance performance = BizUtils.findPerformance(map2);
            performance.setFromData("재무제표");
            performance.setDocNm(docNm);
            performance.setKey(key);
            performance.setAcptUrl(KrxCrawlerHelper.getItemUrl(acptNo));
            performance.param(params);
            if (hasThreeMonthInKey(performance)) {
                performance.setParsedPeriod(BizUtils.findPeriod(performance.getPeriod(), "분기보고서"));
            } else {
                performance.setParsedPeriod(BizUtils.findPeriod(performance.getPeriod(), docNm));
            }
            log.info("재무제표실적 " + performance);

        }

        return params;
    }

    private boolean hasThreeMonthInKey(Performance performance) {


        String revenueKey = performance.getRevenueKey();
        String operatingProfitKey = performance.getOperatingProfitKey();
        String netIncomeKey = performance.getNetIncomeKey();

        if (revenueKey.contains("3개월")) {
            return true;
        }
        if (operatingProfitKey.contains("3개월")) {
            return true;
        }
        if (netIncomeKey.contains("3개월")) {
            return true;
        }

        return false;

    }

    private Map<String, Object> parseFs(FinancialStatementSelectorByPattern selector, TableParser tableParser, String c, String docUrl, MyParser myParser) throws Exception {
        // 재무제표 분석.
        selector.setKeywords(new String[]{"재무", "제표"});
        selector.setNoKeywords(new String[]{"연결"});
        return (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);
    }

    private Map<String, Object> parseCfs(FinancialStatementSelectorByPattern selector, TableParser tableParser, String c, String docUrl, MyParser myParser) throws Exception {
        // 연결재무제표 분석..
        selector.setKeywords(new String[]{"연결", "재무", "제표"});
        selector.setNoKeywords(new String[]{});
        return (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);

    }

    private boolean isPerformanceKongsi(String docNm) {
        if (docNm.contains("[")) {
            String string = StringUtils.substringBetween(docNm, "[", "]");
            if (isEmpty(string)) {

            } else {
                docNm = StringUtils.remove(docNm, "[" + string + "]");
            }
        }

        if (StringUtils.startsWithAny(docNm, "분기보고서", "반기보고서", "사업보고서") == false) {
            return false;
        } else {
            if (docNm.contains("신고서")) {
                return false;
            } else {
                return true;
            }


        }
    }


}
