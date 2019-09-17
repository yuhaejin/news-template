package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Change;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.MajorStockChangeParser;
import com.tachyon.crawl.kind.parser.StaffBirthDayParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByPattern;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByPattern2;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO 국믄연금관련된 문제가 있을 때... 통일된 투자자 이름으로 변경을 해야 함.
 */
@Slf4j
@Component
public class StockHolderCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;

    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Override
    public void execute(Message message) throws Exception {
        String key = myContext.findInputValue(message);
        log.info("<<< " + key);
        if (isSemiRealTimeData(message) == false) {
            log.info("준실시간데이터가 아님.. " + key);
            return;
        }
        // 이미 처리된 공시인지 확인
        // 처리되지 않았으면 공시 수집
        // 공시 StockChange 분석함
        // DB에 저장함.
        // 종료로그...
        log.info("... " + key);
        String[] keys = StringUtils.split(key, "_");
        String docNo = keys[0];
        String code = keys[1];
        String acptNo = keys[2];

        Map<String, Object> map = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
        if (map != null) {
            List<Change> changes = new ArrayList<>();
            Map<String, Change> FILTER = new HashMap<>();
            Map<String, String> DELETE = new HashMap<>();
            String docUrl = Maps.getValue(map, "doc_url");
            log.info("... " + docUrl);
            String docNm = Maps.getValue(map, "doc_nm");
            String rptNm = Maps.getValue(map, "rpt_nm");
            String tnsDt = findTnsDt(map);
            String docRaw = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, loadBalancerCommandHelper);
            // 일반문서처리.
            Table table = null;
            if (isStaffStockStatusKongsi(docNm)) {
                //임원ㆍ주요주주특정증권등소유상황보고서
                log.info("임원ㆍ주요주주특정증권등소유상황보고서 공시처리.. ");
                StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
                TableParser stockChangeTableParser = new TableParser(selector);
                selector.setKeywords(new String[]{"특정", "증권", "소유", "상황"});
                table = (Table) stockChangeTableParser.parse(docRaw, docUrl);
                if (table != null) {
                    String submitName = Maps.getValue(map, "submit_nm");
                    //birthday가 없을 수도 있음
                    String birthDay = findBirthDay(docRaw, docUrl);
                    table.findStock(docUrl, docNo, docNm, tnsDt, rptNm, acptNo, submitName, birthDay, FILTER);
            }
            } else if (isMajorStockChangeKongis(docNm)) {
                log.info("최대주주등소유주식변동신고서 공시처리...");
                TableParser stockChangeTableParser = new TableParser(new StockChangeSelectorByPattern2());
                List<Table> tables = stockChangeTableParser.parseSome(docRaw, docUrl, new MajorStockChangeParser());
                if (tables != null && tables.size() > 0) {
                    for (Table _table : tables) {
                        _table.findMajorStock(docUrl, docNo, docNm, tnsDt, rptNm, acptNo, FILTER);
                    }
                } else {
                    log.info("세부변동내역 테이블 없음. ");
                }
            } else {
                // 일반 문서.
                TableParser stockChangeTableParser = new TableParser(new StockChangeSelectorByPattern());
                table = (Table) stockChangeTableParser.parse(docRaw, docUrl);
                if (table != null) {
                    table.findStock(docUrl, docNo, docNm, FILTER, tnsDt, rptNm, acptNo);
                }
            }

            if (table != null) {
                if (isChangeKongsi(rptNm)) {
                    if (table.getBodies() == null || table.getBodies().size() == 0) {

                    } else {
                        if (table.getBodies().size() > 0) {
                            // 기존 투자자정보 삭제...
                            String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
                            log.info("이전StockHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);
                            if (StringUtils.isEmpty(_docNo) == false) {
                                deleteBeforeStockHolder(templateMapper, code, _docNo);
                                log.info("이전StockHolder 삭제 code=" + code + " docNo=" + _docNo);
                                DELETE.put(_docNo, _docNo);
                            }
                        }
                    }
                }
            }


            for (String _key : FILTER.keySet()) {
                Change change = FILTER.get(_key);
                log.debug(change.toString());
                if (DELETE.containsKey(change.getDocNo()) == false) {
                    change.setAcptNo(acptNo);
                    change.setIsuCd(code);
                    if (isNameTotal(change.getName()) == false) {
                        // 이름이 합계이면 skip..
                        changes.add(change);
                    } else {
                    }
                }
            }

            setupName(changes);
            setupPrice(changes);
            log.info("주식거래내역 ... " + changes.size());
            int stockCount = 0;
            for (Change change : changes) {
                if (StringUtils.isEmpty(change.getName()) || "-".equalsIgnoreCase(change.getName())) {
                    log.info("거래내역에 이름이 없어 SKIP " + change);
                } else {

                    //대표투자자명으로 변경함.
                    modifyRepresentativeName(change);

                    // 유일한 값이라고 할만한 조회...
                    //mybatis 처리시 paramMap을 다른 클래스에서 처리한 것은 나중에 수정..
                    if (findStockHolder(templateMapper, code, change)) {
                        // 정정된 것은 이미 삭제가 되었으므로 업데이트하지 않아도 딱히 문제가 없음.
                        log.info("ALREADY StockHodler " + change);
                    } else {
                        log.info("INSERT ... " + change);
                        insertStockHolder(templateMapper, change.paramStockHolder(code, acptNo));
                        stockCount++;
                    }



                    if (hasExpiration(change)) {
                        log.info("임기만료 ... "+change);
                        handleExpiration(templateMapper, change.paramExpiration(DateUtils.toString(change.getDateType(),"yyyyMMdd")));
                    }

                }
            }

            if (stockCount > 0) {
                int count = templateMapper.findTelegramHolder(docNo, acptNo, "NOT_KEYWORD");
                if (count == 0) {
                    templateMapper.insertTelegramHolder(docNo, code, acptNo, "NOT_KEYWORD");
                } else {
                }
            }
        } else {
            log.error("기초공시가 없음  docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
        }




        log.info("done " + key);
    }

    private void modifyRepresentativeName(Change change) {
        String ownerName = change.getName().trim();
        if (myContext.hasRepresentativeName(ownerName)) {
            String _ownerName = myContext.findRepresentativeName(ownerName);
            log.info(ownerName+" ==> "+_ownerName);
            change.setName(_ownerName);

        }
    }

    private void handleExpiration(TemplateMapper templateMapper, Map<String, Object> param) {
        int count = templateMapper.findExpirationCount(param);
        if (count == 0) {
            templateMapper.insertExpiration(param);
        }
    }

    private boolean hasExpiration(Change change) {
        String remarks = change.getRemarks();
        if (StringUtils.contains(remarks, "만료") && StringUtils.contains(remarks, "임기")) {
            return true;
        } else {
            return false;
        }
    }

    private void setupPrice(List<Change> changes) {
        for (Change change : changes) {
            if (change.getPrice() == 0) {
                log.debug("  "+change.getDate());
                Integer price = templateMapper.findClose(change.getIsuCd(),new Timestamp(change.getDateTime()));
                if (price == null || price == 0) {
                    log.debug("no...");
                } else {
                    log.debug("0 ==>  " + price);
                    change.setPrice(price);
                    change.setupProfit();
                }
            }
        }
    }

    private String toString(Timestamp date) {
        return DateUtils.toString(date, "yyyy-MM-dd HH:mm:ss");
    }


    private boolean isStaffStockStatusKongsi(String docNm) {
        if (docNm.contains("임원") && docNm.contains("주요주주") && docNm.contains("특정증권등") && docNm.contains("소유상황")) {
            return true;
        } else {
            return false;
        }
    }


    private String findBirthDay(String docRaw, String docUrl) throws Exception {
        StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
        selector.setKeywords(new String[]{"보고자", "관한", "사항"});
        TableParser stockChangeTableParser = new TableParser(selector);
        List<Table> tables = stockChangeTableParser.parseSome(docRaw, docUrl, new StaffBirthDayParser());
        if (tables == null || tables.size() == 0) {
            return "";
        } else {
            Table table = tables.get(0);
            return table.findBirthDay();
        }
    }

    private void insertStockHolder(TemplateMapper templateMapper, Map<String, Object> map) {
        templateMapper.insertStockHolder(map);
    }

    private void deleteBeforeStockHolder(TemplateMapper templateMapper, String code, String _docNo) {
        templateMapper.deleteBeforeStockHolder(code, _docNo);
    }

    private boolean isNameTotal(String name) {
        String _name = StringUtils.remove(name, " ");
        if ("합계".equalsIgnoreCase(_name)) {
            return true;
        } else {
            return false;
        }
    }

    public boolean findStockHolder(TemplateMapper templateMapper, String code, Change change) {
        Map<String, Object> param = BizUtils.changeParamMap(change, code);
        int count = templateMapper.findStockHolder(param);
        if (count > 0) {
            return true;
        } else {

            return false;
        }
    }



}
