package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Change;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.*;
import com.tachyon.crawl.kind.parser.handler.SimpleSelectorByPattern;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByPattern;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByPattern2;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.tachyon.crawl.BizUtils.findIndex;

/**
 *
 */
@Slf4j
@Component
public class StockHolderCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RetryTemplate retryTemplate;
    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    /**
     * 투자자명을 변경을 함. 중복되지 않도록 하기 위해서.
     * ==> MyContext에서 처리함.
     */

    static {
//        NAMES.put("One Equity Partners IV, L.P.", "ONE EQUITY PARTNERS Ⅳ,L.P.");
//        NAMES.put("리잘커머셜뱅킹코퍼레이션", "RIZAL COMMERCIAL BANKING CORPORATION");
//        NAMES.put("이스트브릿지아시안미드마켓오퍼튜니티펀드엘피", "EastBridge Asian Mid-Market Opportunity Fund, L.P.");
//
    }

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
            Map<String, Change> FILTER = new LinkedHashMap<>();
            Map<String, String> DELETE = new HashMap<>();
            String docUrl = Maps.getValue(map, "doc_url");
            log.info("... " + docUrl);
            String docNm = Maps.getValue(map, "doc_nm");
            String rptNm = Maps.getValue(map, "rpt_nm");
            String tempRptNm = Maps.getValue(map, "temp_rpt_nm");
            String codeNm = Maps.getValue(map, "isu_nm");
            String tnsDt = findTnsDt(map);
            String docRaw = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, retryTemplate, loadBalancerCommandHelper);
            // 일반문서처리.
            log.info("... done docRow");
            if (validate(docRaw) == false) {
                return;
            }

            // 정정공시중에 이전 공시는 처리하지 않는다.
            if (isOldAtCorrectedKongsi(map)) {
                log.info("SKIP 정정공시중에 이전공시임. .. " + key);
                return;
            }

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
                    Map<String, Object> _table = findBirthDay(docRaw, docUrl);
                    //공시에 있는 생년월일 데이터를 사용하므로 추후 변경처리가 필요할 수도 있다.
                    String birthDay = Maps.findValueAndKeys(_table, "생년월일");
                    String birthDay2 = birthDay;
                    if (isEmpty(submitName)) {
                        // 가끔 제출인명이 없는 경우가 있는데 그때는 아래 데이터를 사용한다.
                        submitName = Maps.findValueAndKeys(_table, "성명", "한글");
                    }

                    table.findStock(docUrl, docNo, docNm, tnsDt, rptNm, acptNo, submitName, birthDay, birthDay2, FILTER);
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
                            List<String> _docNos = findBeforeKongsi(templateMapper, docNo, code, acptNo);
                            for (String _docNo : _docNos) {
                                log.info("이전StockHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);
                                deleteBeforeStockHolder(templateMapper, _docNo,code);
                                log.info("이전StockHolder 삭제 code=" + code + " docNo=" + _docNo);
                                DELETE.put(_docNo, _docNo);
                                deleteBeforeArticle(templateMapper, _docNo,code, acptNo,findArticleType());
                            }
                        }
                    }
                }
            }

            for (String _key : FILTER.keySet()) {
                Change change = FILTER.get(_key);

                if (DELETE.containsKey(change.getDocNo()) == false) {
                    change.setAcptNo(acptNo);
                    change.setIsuCd(code);

                    if (isNameTotal(change.getName())) {
                        // 이름이 합계이면 skip..
                        log.debug("이름 합계 ");
                        continue;
                    }
                    if (hasEmptyValue(change)) {
                        continue;
                    }
                    log.debug("<<< " + change);
                    changes.add(change);
                }
            }

            setupName(changes, myContext);
            setupPrice(changes, codeNm);
            setupRepresentativeName(changes);
            setupBistowal(changes);
            setupBirthYm(changes);
            setupSpecialRelationship(changes,codeNm,docUrl,docRaw);
//            if (changes.size() > 0) {
//                return;
//            }
            String totalStockCount = findTotalStockCount(changes, docRaw, docNm, docUrl);

            List<Map<String, Object>> childParams = new ArrayList<>(); // 자펀드 정보를 담는 객체.
            Long parentSeq = findParentFund(rptNm, docNo, code, acptNo, docUrl, key, childParams);

            int stockCount = 0;

            boolean isCompressedData = findCompressedData(changes, rptNm, docUrl, docRaw);
            if (isCompressedData) {
                log.info("압축된 StockData " + docUrl);
            } else {
                log.info("압축된 StockData가 아님. " + docUrl);
            }
            // 거래자별로 기사 작성을 위한 맵..
            Map<String, String> ownerNameMap = new HashMap<>();


            for (Change change : changes) {
                // 신규보고인데... 실제로는 아닌 경우는 SKIP..
                if (checkFakeNewReport(change)) {
                    log.info("Fake신규보고 SKIP " + change);
                    continue;
                }
                if (StringUtils.isEmpty(change.getName()) || "-".equalsIgnoreCase(change.getName())) {
                    log.info("거래내역에 이름이 없어 SKIP " + change);
                } else {
                    log.debug(change.toString());
                    if (isBirthdayUpdate(message)) {
                        // 생년월일 보정 작업
                        if (findStockHolder(templateMapper, code, change)) {
                            Map<String, Object> _map = toParam(change, code);
                            if (StringUtils.equals(change.getBirthDay(), change.getBirthDay2()) == false) {
                                log.info("updateStockHolderBirthDay " + _map);
                                updateStockHolderBirthDay(templateMapper, _map);
                            } else {
                                log.info("SKIP 생일파싱이 동일함 " + _map);
                            }
                        }

                    } else if (isPriceUpdate(message)) {
                        // 단가 보정
                        if (change.getPrice() == 0) {
                            //"보정된 단가가 0이면 처리하지 않음."
                            continue;
                        }

                        Map<String, Object> _map = toUpdatePriceParam(change, code);
                        log.info("단가보정 ... " + _map + " " + docNo + " " + code);
                        updateStockHolderUnitPrice(templateMapper, _map);


                    } else if (isParentFundUpdate(message)) {
                        if (rptNm.contains("주식등의대량보유상황보고서") == false) {
                            log.info("모펀드보정 주식등의대량상황보고서가 아님.  " + key);
                            break;
                        }

                        Long seq = findStockHolderSeq(change, code, docNo);
                        if (seq == null) {
                            log.info("모펀드보정할 거래가 없음.. " + change);
                            continue;
                        } else {
                            if (parentSeq == null) {
                                log.info("모펀드 정보가 없음. " + key);
                                updateParentFund(templateMapper, seq, parentSeq);
                            } else {

                                String birth = change.getBirthDay();
                                log.debug("모펀드Seq 찾기..  " + birth);
                                if (hasChildFund(birth, childParams) == false) {
                                    updateParentFund(templateMapper, seq, null);
                                } else {
                                    updateParentFund(templateMapper, seq, parentSeq);
                                }
                            }
                        }

                    } else if (isMajorStockUpdate(message)) {
                        // 최대주주등소유주식변동신고서 의 거래자명을 제출인이 아닌 성명으로 보정처리함.

//                        String submitName = Maps.getValue(map, "submit_nm");
//                        if (StringUtils.equals(submitName, change.getName())) {
//                            log.info("SKIP 거래자명 동일 " + change);
//                            continue;
//                        }
                        String name = change.getName();
                        if (name.contains("(") == false || name.contains(")") == false) {
                            log.info("거래명 처리대상이 아님.. " + change.getName());
                            continue;
                        }
                        String string = StringUtils.substringBetween(name, "(", ")");

                        if (string.contains("특") == false) {
                            log.info("거래명 처리대상이 아님.. " + change.getName());
                            continue;
                        }
                        name = BizUtils.removeBracket(name);

                        Map<String, Object> param = BizUtils.changeParamMap(change, code);
                        param.put("owner_name", name);

                        List<Map<String, Object>> maps = templateMapper.findStockName(param);
                        if (maps == null || maps.size() == 0) {
                            continue;
                        }
                        for (Map<String, Object> _map : maps) {
                            Long seq = Maps.getLongValue(_map, "seq");
                            String ownerName = Maps.getValue(_map, "owner_name");
                            if (StringUtils.equals(ownerName, change.getName()) == false) {
                                log.info("거래자명변경 " + seq + " " + ownerName + " >>  " + change.getName());
                                templateMapper.updateStockHolderName(seq, change.getName());
                            } else {
                                log.info("SKIP 거래자명동일 " + seq + " " + ownerName);
                            }
                        }

                    } else {
                        // 유일한 값이라고 할만한 조회...
                        //mybatis 처리시 paramMap을 다른 클래스에서 처리한 것은 나중에 수정..
                        Map<String, Object> findParam = param(change, code, tempRptNm, docUrl, docNo, acptNo);
                        Long seq = hasDuplicateStockHolder(templateMapper, findParam);
                        if (seq != null) {
                            // 정정된 것은 이미 삭제가 되었으므로 업데이트하지 않아도 딱히 문제가 없음.
                            log.info("ALREADY StockHodler " + change);
                            if (isCompressedData) {
                                log.info("Compress StockData " + seq);
                                compressStockHolder(templateMapper, seq);
                            }

                        } else {
                            Map<String, Object> param = change.paramStockHolder(code, acptNo);
                            setupCompressed(param, isCompressedData);
                            setupYesterDayClosePrice(param, change);
                            setupParentFun(param, parentSeq, childParams);
                            setupTotalStockCount(param, totalStockCount);
                            setupSpecialRelationship(param, change.getJson());
                            //거래마다 처리하면 모펀드 데이터가 각 거래마다 달라질 수 있어 삭제처리.
//                            setupParentFund(param, change);
                            log.info("INSERT ... " + param);
                            insertStockHolder(templateMapper, param);
                            modifyParam(findParam);
                            if (change.getChangeAmount() != 0) {
                                // 변동량이 있을 때만 기사 작성하기.. by sokhoon 20200605
                                if (isGoodArticle(docNm)) {
                                    String ownerName = Maps.getValue(param, "owner_name");
                                    String birthDay = Maps.getValue(param, "birth_day");
                                    String _key = ownerName + "_" + birthDay;
                                    if (ownerNameMap.containsKey(_key) == false) {
                                        ownerNameMap.put(_key, _key);
                                    }
//                                    sendToArticleQueue(rabbitTemplate,findPk(param),"STOCK",findParam);
                                }
                            }
                            stockCount++;
                        }

                        if (hasExpiration2(change)) {
                            log.info("임기만료 ... " + change);
                            handleExpiration2(templateMapper, createParam(change));
                        }
                    }


                    handleGiveNTake(change, docNm);

                }
            }

            // 투자자별로 기사 생성할 수 있게 처리함.
            if (ownerNameMap.size() > 0) {
                for (String ownerName : ownerNameMap.keySet()) {
                    sendToArticleQueue(rabbitTemplate, key, "STOCK", ownerName);
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

    private void setupSpecialRelationship(Map<String, Object> param, String json) {
        param.put("spare_data", json);
    }

    /**
     * 특별관계 해소 거래가 있으면 추가 분석을 함.
     * 발행회사,보고자 : 이 부분은 공시정보로 확인이 가능함.
     * 직전보고서 보유비율
     * 이번보고서 보유비율
     * 변동사유 : 공동 보유 관계 해소와 대표 보고자 변경
     * 보유목적 : 단순투자인지 또는 주주 권리를 행사 (이부분은 추가 가이드 필요)
     * @param changes
     */
    private void setupSpecialRelationship(List<Change> changes,String codeNm,String docUrl,String c) throws Exception {
        boolean hasSpecialRelationship = hasSpecialRelationship(changes);
        if (hasSpecialRelationship) {
            // 분석처리
            log.info("특별관계해소 거래를 가지고 있음. "+docUrl);
            String company = codeNm;
            StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
            selector.setKeywords(new String[]{"주식등의", "대량보유상황보고서"});

            TableParser stockChangeTableParser = new TableParser(selector);
            List<List<String>> lists = (List<List<String>>)stockChangeTableParser.simpleParse(c, docUrl, new RawTablesrMyParser());
            String reportReason = findReportReason(lists);
            String beforeRatio = findBeforeRatio(lists);
            String afterRatio = findAfterRatio(lists);
            String purpose = findPurpose(lists);

            for (Change change : changes) {
                if (isSpecialRelationship(change)) {
                    String json = convertToJson(company, change.getName(), reportReason, beforeRatio, afterRatio, purpose);
                    change.setJson(json);
                    log.info("특별관계해소 데이터 "+json);
                }
            }
            // 분석후 해당 거래에 값(json형태로) 설정
            // DB에 저장하게 함.
        }

    }

    private String convertToJson(String company, String name, String reportReason, String beforeRatio, String afterRatio, String purpose) {

        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("company", company);
        jsonMap.put("ownerName", name);
        jsonMap.put("reportReason", reportReason);
        jsonMap.put("beforeRatio", beforeRatio);
        jsonMap.put("afterRatio", afterRatio);
        jsonMap.put("purpose", purpose);

        return JsonUtils.toJson(jsonMap);

    }
    private String findPurpose(List<List<String>> lists) {
        for (List<String> list : lists) {
            int index = findIndex(list, "보유목적");
            if (index >= 0) {
                if (index != list.size() - 1) {
                    return list.get(index + 1);
                } else {
                    return "";
                }
            }
        }
        return "";
    }
    private String findRatio(List<List<String>> lists,String keyword1,String keyword2) {
        for (List<String> list : lists) {
            int _index = findIndex(list, keyword1);
            if (_index < 0) {
                continue;
            }
            int index = findIndex(list, keyword2);
            if (index >= 0) {
                if (index != list.size() - 2) {
                    return list.get(index + 1)+"_"+list.get(index+2);
                } else {
                    return "";
                }
            }
        }
        return "";
    }


    private String findAfterRatio(List<List<String>> lists) {
        return findRatio(lists, "보유주식", "이번보고서");
    }

    private String findBeforeRatio(List<List<String>> lists) {
        return findRatio(lists, "보유주식", "직전보고서");
    }

    private String findReportReason(List<List<String>> lists) {
        return "공동 보유 관계 해소와 대표 보고자 변경";
    }
    private boolean hasSpecialRelationship(List<Change> changes) {
        for (Change change : changes) {
            if (isSpecialRelationship(change)) {
                return true;
            }
        }

        return false;
    }

    private boolean isSpecialRelationship(Change change) {
        String etc = change.getEtc();
        if (StringUtils.containsAny(etc, "공동보유", "대표보고자")) {
            return true;
        } else {
            return false;
        }



    }

    @Override
    public String findArticleType() {
        return "STOCK";
    }

    private void setupTotalStockCount(Map<String, Object> param, String totalStockCount) {
        if (isEmpty(totalStockCount)) {
            param.put("no_of_share", 0);
        } else {
            param.put("no_of_share", Long.valueOf(totalStockCount));
        }
    }

    private String findAcptNoIsuCd(Change change) {
        return change.getAcptNo() + "_" + change.getIsuCd();
    }

    private boolean isCorrectTotalStockCount(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__UPDATE")) {
            if ("TOTAL_STOCK_COUNT".equalsIgnoreCase((String) message.getMessageProperties().getHeaders().get("__UPDATE"))) {
                return true;
            }
        }
        return false;
    }

    private String findTotalStockCount(List<Change> changes, String kongsi, String docNm, String docUrl) throws Exception {
        if (changes.size() == 0) {
            return null;
        }

        Map<String, Object> map = null;
        if (isStaffStockStatusKongsi(docNm) || isDisclosureInformation(docNm)) {
            //임원ㆍ주요주주특정증권등소유상황보고서, 주식등의 대량상황보고서
            StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
            selector.setKeywords(new String[]{"회사", "관한", "사항"});
            TableParser stockChangeTableParser = new TableParser(selector);
            map = (Map<String, Object>) stockChangeTableParser.simpleParse(kongsi, docUrl, new StaffInfoParser());


        } else if (isMajorStockChangeKongis(docNm)) {
            // 최대주주등소유주식변동신고서
            StockChangeSelectorByPattern2 pattern2 = new StockChangeSelectorByPattern2();
            pattern2.setKeywords(new String[]{"발행", "주식"});
            TableParser stockChangeTableParser = new TableParser(pattern2);
            TotalStockParser myParser = new TotalStockParser();
            map = (Map<String, Object>) stockChangeTableParser.simpleParse(kongsi, docUrl, myParser);
            //발행주식총수
        } else {

        }

        if (map != null) {
            //의결권있는발행주식총수,발행주식총수,발행주식총수
            String count = findCount(map);
            if (isEmpty(count)) {
                return null;
            } else {
                // , 삭제처리.
                return StringUtils.remove(count, ",");
            }
        } else {
            return null;
        }

    }

    private String findCount(Map<String, Object> map) {
        List<String> strings = Maps.findValuesAndKeys(map, "주식", "총수", "발행");
        if (strings == null || strings.size() == 0) {
            return "";
        }
        if (strings.size() == 1) {
            return strings.get(0);
        }

        for (String s : strings) {
            s = StringUtils.remove(s, ",");
            if (StringUtils.isNumeric(s)) {
                return s;
            }
        }

        return "";
    }

    private boolean isDisclosureInformation(String docNm) {
        if (docNm.contains("주식") && docNm.contains("대량") && docNm.contains("보유") && docNm.contains("상황")) {
            return true;
        } else {
            return false;
        }

    }

    private boolean isMajorStockUpdate(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__UPDATE")) {
            if ("MAJORSTOCK".equalsIgnoreCase((String) message.getMessageProperties().getHeaders().get("__UPDATE"))) {
                return true;
            }
        }
        return false;
    }

    private void updateParentFund(TemplateMapper templateMapper, Long seq, Long parentSeq) {
        if (parentSeq == null) {
            log.info("모펀드정보가 없어 parentSeq null 처리 at " + seq);
        } else {
            log.info("모펀드보정 parentSeq=" + parentSeq + " at " + seq);
        }
        templateMapper.updateParentFund(seq, parentSeq);
    }


    private Long findStockHolderSeq(Change change, String code, String docNo) {
        Map<String, Object> param = BizUtils.changeParamMap(change, code);
        List<Map<String, Object>> maps = templateMapper.findStockHolderSeq(param);
        if (maps == null || maps.size() == 0) {
            return null;
        } else {
            if (maps.size() == 1) {
                Map<String, Object> map = maps.get(0);
                return Maps.getLongValue(map, "seq");
            } else {
                for (Map<String, Object> map : maps) {
                    Long seq = Maps.getLongValue(map, "seq");
                    String _docNo = Maps.getValue(map, "doc_no");
                    if (_docNo.equalsIgnoreCase(docNo)) {
                        return seq;
                    }
                }

                return null;
            }
        }
    }

    private boolean hasChildFund(String birth, List<Map<String, Object>> childParams) {
        for (Map<String, Object> map : childParams) {
//            String _name = Maps.getValue(map, "ename");
            String _birth = Maps.getValue(map, "birth");
            log.debug("자펀드정보존재여부 " + _birth + " << " + birth);
            if (birth.equalsIgnoreCase(_birth)) {
                return true;
            }
        }

        return false;
    }

    private boolean isParentFundUpdate(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__UPDATE")) {
            if ("FUND".equalsIgnoreCase((String) message.getMessageProperties().getHeaders().get("__UPDATE"))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 모자 펀드를 왜 구분을 했는지 알수는 없으나
     * tdr의 요구사항은 구분이 필요하지는 않다고 한다.
     *
     * @param param
     * @param parentSeq
     */
    private void setupParentFun(Map<String, Object> param, Long parentSeq, List<Map<String, Object>> childParams) {
//        String name = Maps.getValue(param,"owner_name");
        String birthDay = Maps.getValue(param, "birth_day");
        if (hasChildFund(birthDay, childParams)) {
//            log.debug("모펀드Seq 찾기 OK " + birthDay + " " + parentSeq);
            param.put("prnt_seq", parentSeq);
        } else {
            // 모자펀드를 구분하기 위해서 이렇게 처리한건가?
            // 굳이 모자펀드를 구분할 필요는 없다고 한다. tdr에서
            param.put("prnt_seq", parentSeq);
//            log.debug("모펀드Seq 찾기 NO " + birthDay + " " + parentSeq);
        }
    }

    /**
     * 모자 펀드 데이터 분석 및 처리.
     *
     * @param rptNm
     * @param docNo
     * @param isuCd
     * @param acptNo
     * @param docUrl
     * @param key
     * @return
     * @throws Exception
     */
    private Long findParentFund(String rptNm, String docNo, String isuCd, String acptNo, String docUrl, String key, List<Map<String, Object>> childParams) throws Exception {
        if (rptNm.contains("주식등의대량보유상황보고서") == false) {
            return null;
        }
        String htmlPath = findPath(myContext.getHtmlTargetPath(), isuCd, docNo, "htm");
        File f = new File(htmlPath);
        if (f.exists() == false) {
            log.info("파일이 없음. " + htmlPath + " " + key);
            return null;
        }
        SimpleSelectorByPattern selector = new SimpleSelectorByPattern();
        selector.setKeywords(new String[]{"대량", "보유자", "사항"});
        TableParser tableParser = new TableParser(selector);
        ParentFundMyParser myParser = new ParentFundMyParser();

        String c = FileUtils.readFileToString(f, "UTF-8");

        List<Map<String, Object>> tables = (List<Map<String, Object>>) tableParser.simpleParse(c, docUrl, myParser);
        if (tables == null || tables.size() <= 1) {
            log.info("분석해보니 관련테이블 정보가 없음. " + key);
            return null;
        }
        Map<String, Object> parentFund = findParentFund(tables);
        log.info("모펀드 " + parentFund);
        if (parentFund == null) {
            log.info("모펀드 관련테이블 정보가 없음. " + key);
            return null;
        }
        List<Map<String, Object>> childFunds = findChildFunds(tables);
        if (childFunds.size() == 0) {
            log.info("자펀드 관련테이블 정보가 없음. " + key);
//            return null;
        }

        for (Map<String, Object> child : childFunds) {
            log.info("자펀드 " + child);
        }

        Map<String, Object> parentParam = convertParentFund(parentFund);
        log.info("분석모펀드 " + parentParam);
        setupKongsi(parentParam, docNo, isuCd, acptNo);
        convertChildFunds(childFunds, childParams);
        if (childParams.size() == 0) {
            // 자펀드 데이터가 없을 때 모펀드 정보를 삽입함..
            childParams.add(parentParam);
        }
        for (Map<String, Object> child : childParams) {
            log.info("분석자펀드 " + child);
        }

        Long seq = templateMapper.findParentFund(parentParam);

        if (seq == null) {
            templateMapper.insertParentFund(parentParam);
            Long _seq = (Long) parentParam.get("seq");
            for (Map<String, Object> param : childParams) {
                param.put("prnt_seq", _seq);
                if (templateMapper.countChildFund(param) == 0) {
                    templateMapper.insertChildFund(param);
                }
            }
            return _seq;
            // childparam 도 insert
        } else {
            for (Map<String, Object> param : childParams) {
                param.put("prnt_seq", seq);
                if (templateMapper.countChildFund(param) == 0) {
                    templateMapper.insertChildFund(param);
                }
            }

            return seq;
        }


    }

    private int findChildRows(List<Map<String, Object>> maps) {
        int count = 0;

        return 0;
    }

    /**
     * name 200
     * type
     * relation
     * birth 30
     * nationality
     * address 500
     * job
     * etc
     *
     * @param maps
     * @return
     */
    private void convertChildFunds(List<Map<String, Object>> maps, List<Map<String, Object>> params) {

        for (Map<String, Object> map : maps) {
            String type = Maps.findValueAndKeys(map, "구분");
            if (type.contains("외국") == false) {
                continue;
            }
            String seq = Maps.getValue(map, "연번");
            if (isEmpty(seq)) {
                continue;
            }
            if ("-".equalsIgnoreCase(seq.trim())) {
                continue;
            }
            Map<String, Object> param = new HashMap<>();
            String ename = Maps.findValueOrKeys(map, "성명", "명칭");
            param.put("ename", ename);
            param.put("type", type);
            param.put("relation", Maps.findValueAndKeys(map, "보고자", "관계"));
            param.put("birth", removeSpace(Maps.findValueOrKeys(map, "생년월일", "사업자등록번호")));
            param.put("nationality", Maps.findValueAndKeys(map, "국적"));
            param.put("address", Maps.findValueAndKeys(map, "주소"));
            param.put("job", Maps.findValueAndKeys(map, "직업"));
            param.put("etc", Maps.findValueAndKeys(map, "발행회사"));
            param.put("modi_name", modifyName(ename));

            params.add(param);

        }

    }

    private String modifyName(String ename) {
        if (isEmpty(ename)) {
            return "";
        }
        ename = ename.toLowerCase();
        return StringUtils.remove(ename, " ");


    }

    private void setupKongsi(Map<String, Object> parentParam, String docNo, String code, String acptNo) {
        parentParam.put("doc_no", docNo);
        parentParam.put("isu_cd", code);
        parentParam.put("acpt_no", acptNo);
    }


    /**
     * hname 200
     * ename 100
     * address
     * birth 30
     * job
     * bc_company 200
     * bc_name
     * bc_spot
     *
     * @param map
     * @return
     */
    private Map<String, Object> convertParentFund(Map<String, Object> map) {
        Map<String, Object> param = new HashMap<>();
        param.put("hname", Maps.findValueAndKeys(map, "성명", "한글"));
        String ename = Maps.findValueAndKeys(map, "성명", "영문");
        param.put("ename", ename);
        param.put("address", Maps.findValueAndKeys(map, "주소", "본점"));
        param.put("birth", removeSpace(Maps.findValueAndKeys(map, "생년월일")));
        param.put("job", Maps.findValueAndKeys(map, "직업"));
        param.put("bc_company", Maps.findValueAndKeys(map, "업무상", "회사"));
        param.put("bc_name", Maps.findValueAndKeys(map, "업무상", "성명"));
        param.put("bc_spot", Maps.findValueAndKeys(map, "업무상", "직위"));
        param.put("type", "외국법인");
        param.put("modi_name", modifyName(ename));

        return param;
    }

    private String removeSpace(String s) {
        return StringUtils.remove(s, " ");
    }

    private List<Map<String, Object>> findChildFunds(List<Map<String, Object>> tables) {
        List<Map<String, Object>> maps = new ArrayList<>();
        for (Map<String, Object> map : tables) {
            if (map.containsKey("연번") || map.containsKey("구분")) {
                String v = Maps.getValue(map, "구분");
                if (v.contains("외국")) {
                    maps.add(map);
                }
            }
        }

        return maps;

    }

    private Map<String, Object> findParentFund(List<Map<String, Object>> tables) {
        for (Map<String, Object> map : tables) {
            log.debug(map.toString());
            if (map.containsKey("보고자구분") && map.containsKey("직업(사업내용)")) {
                String v = Maps.getValue(map, "보고자구분");
                if (v.contains("외국")) {
                    // 개인은 필터링해야 하나? FIXME
                    if (v.contains("개인")) {
                        log.info("SKIP 개인이어서 " + v);
                    } else {
                        return map;
                    }
                }
            }
        }

        return null;
    }

//    /**
//     * 사업자인 경우 모자펀드 정보를 조회해서 처리한다.
//     *
//     * @param param
//     * @param change
//     */
//    private void findParentFund(Map<String, Object> param, Change change) {
//        String birth = change.getBirthDay();
//        Long parentSeq = templateMapper.findParentFundSeq(birth);
//
//        param.put("prnt_seq", parentSeq);
//    }

    private void updateStockHolderUnitPrice(TemplateMapper templateMapper, Map<String, Object> map) {
        templateMapper.updateStockHolderUnitPrice(map);
    }

    private Map<String, Object> toUpdatePriceParam(Change change, String code) {
        Map<String, Object> map = BizUtils.changeParamMap(change, code);
        map.put("unit_price", change.getPrice());
        map.put("unit_price2", change.getPrice2());
        return map;
    }

    private boolean isPriceUpdate(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__UPDATE")) {
            if ("PRICE".equalsIgnoreCase((String) message.getMessageProperties().getHeaders().get("__UPDATE"))) {
                return true;
            }
        }
        return false;
    }

    private void modifyParam(Map<String, Object> findParam) {
        findParam.remove("doc_url");
        findParam.remove("acpt_no");
        findParam.remove("doc_no");
        findParam.remove("isu_cd");
    }

    private Map<String, Object> param(Change change, String code, String tempRptNm, String docUrl, String docNo, String acptNo) {
        Map<String, Object> param = BizUtils.changeParamMap(change, code);
        param.put("temp_rpt_nm", tempRptNm);
        param.put("doc_url", docUrl);
        param.put("doc_no", docNo);
        param.put("isu_cd", code);
        param.put("acpt_no", acptNo);

        return param;
    }

    /**
     * 어제 종가와 비교해서 +-30% 이상 차이가 나면 odd_value_yn 에 Y설정함.
     * 오늘이면서 단가가 0이면 SKIP...
     * 전날 종가가 없으면 SKIP..
     * 거래일이 주말이면 SKIP
     *
     * @param param
     * @param change
     */
    private void setupYesterDayClosePrice(Map<String, Object> param, Change change) {
        param.put("odd_value_yn", "N");
        String _code = change.getIsuCd();
        String stockMethod = change.getStockMethod();
        if (BizUtils.isSkipStockMethod(stockMethod)) {
            log.debug("SKIP 처리하지 않아도 될 주식 주말이어서 처리하지 않음 " + change);
            return;
        }

        long price = change.getPrice();
        Date changeDate = new Date(change.getDateTime());
        if (price == 0) {
            // price가 0 인데 오늘이면 SKIP
            if (DateUtils.isToday(changeDate)) {
                log.debug("SKIP 오늘은 종가가 없어 처리하지 않음 " + change);
                return;
            }
            // 거래일이 주말이면 SKIP...
            if (DateUtils.isWeekend(changeDate)) {
                log.debug("SKIP 거래일이 주말이어서 처리하지 않음 " + change);
                return;
            }
        }

        Date yesterDay = findYesterDay(changeDate);
        Timestamp _yesterDay = new Timestamp(yesterDay.getTime());

        String code = BizUtils.findCode(_code, stockMethod);
        Integer closePrice = templateMapper.findCloseByStringDate(code, DateUtils.toString(_yesterDay,"yyyyMMdd"));
        if (closePrice == null) {
            log.warn("종가를 찾을 수 없음. " + code + " " + _yesterDay);
            return;
        }
        long value = Math.abs(price - closePrice);
        int percentage = percentage(value, closePrice);
        if (percentage >= 30) {
            param.put("odd_value_yn", "Y");
            param.put("before_price", closePrice + "");
            String name = change.getName();
            log.warn("전날종가보다 +-30% 임 종목명=" + code + " 변동일=" + change.getDate() + " " + name + " 전날=" + DateUtils.toString(yesterDay, "yyyy-MM-dd") + " 전날종가=" + closePrice + " 현단가=" + price + " percentage=" + percentage + " " + change.getDocUrl());
            //TODO 텔레그램 알림..
        } else {
            log.debug("전날종가보다 +=30% 이하임..");
        }
    }


    /**
     * 전날을 구한다.
     * 다만 웡요일이면 금요일을 구해야 함.
     *
     * @param date
     * @return
     */
    private Date findYesterDay(Date date) {
        if (isMonday(date)) {
            return DateUtils.addDays(date, -3);
        } else {
            return DateUtils.addDays(date, -1);
        }
    }

    private boolean isMonday(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int dayNum = cal.get(Calendar.DAY_OF_WEEK);
        if (dayNum == 2) {
            return true;
        } else {
            return false;
        }
    }

    private int percentage(double value, double closePrice) {
        return (int) (value * 100 / closePrice);
    }

    private String modifyDate(long dateTime) {
        return DateUtils.toString(new Date(dateTime), "yyyyMMdd");
    }

    /**
     * 거래내역이 단건이 아닌 여러건을 한번에 압축하여 처리한 것인지 표기한다.
     *
     * @param templateMapper
     * @param seq
     */
    private void compressStockHolder(TemplateMapper templateMapper, Long seq) {
        templateMapper.compressStockHolder(seq);
    }

    /**
     * 압축된 여부를 적용.
     *
     * @param param
     * @param isCompressedData 세부변동내역에 여러 데이터를 합축된 경우가 있는데 그 여부를 확인.
     */
    private void setupCompressed(Map<String, Object> param, boolean isCompressedData) {
        if (isCompressedData) {
            param.put("is_compressed", "Y");
        } else {
            param.put("is_compressed", "N");
        }
    }

    /**
     * 주식등의 대량상황보고서 중에 압축한 STOCK 데이터 여부를 확인함..
     *
     * @param changes
     * @param rptNm
     * @param docUrl
     * @param contents
     * @return
     */
    private boolean findCompressedData(List<Change> changes, String rptNm, String docUrl, String contents) {
        if (rptNm.contains("주식등의대량보유상황보고서")) {
            if (changes.size() == 1) {
                Change change = changes.get(0);
                Table table = findTable(contents, docUrl);
                if (table == null) {
                    log.debug("[압축STOCKDATA] 분석할 테이블이 없음.... ");
                    return false;
                }
                String[] strings = findBeforeNowDate(table.toMapList());
                String before = strings[0];
                // 공시의 이번보고서 작성기준일은 사용하지 않음. 현재 처리하는게 이미 DB에 존재할 수 있기 때문에..
//                String now = strings[1];
                if (StringUtils.isAnyEmpty(before)) {
                    log.debug("[압축STOCKDATA] 직전(이번)보고서 작성기준일이 없음. ... ");
                    return false;
                } else {
                    String beforeDate = convertNumberDate(before);
                    String nowDate = convertNumberDate(change.getDateTime());
                    if (StringUtils.isAnyEmpty(beforeDate, nowDate)) {
                        log.debug("[압축STOCKDATA] 직전(이번)보고서 작성기준일을 찾을 수 없음.... ");
                        return false;
                    } else {
                        String code = change.getIsuCd();
                        String name = change.getName();
                        int count = templateMapper.findBeforeStock(code, name, beforeDate, nowDate);
                        if (count > 0) {
                            log.debug("[압축STOCKDATA] 범위내에 이미 StockData 존재하여 압축된 데이터임..... ");
                            return true;
                        } else {
                            log.debug("[압축STOCKDATA] 범위내에 StockData 존재하지 않아 압축된 데이터가 아님..... ");
                            return false;
                        }
                    }
                }
            } else {
                log.debug("[압축STOCKDATA] 변동내역이 두개 이상임... ");
                return false;
            }
        } else {
            log.debug("[압축STOCKDATA] 주식등의대량보유상황보고서가 아님.. ");
            return false;
        }
    }

    private String convertNumberDate(Long dateTime) {
        return DateUtils.toString(new Date(dateTime), "yyyyMMdd");
    }

    private String convertNumberDate(String date) {

        List<String> list = BizUtils.convertToList(date);
        if (list.size() != 3) {
            return "";
        }
        String y = list.get(0);
        if (y.length() == 2) {
            y = "20" + y;
        }

        String m = list.get(1);
        if (m.length() == 1) {
            m = "0" + m;
        }

        String d = list.get(2);
        if (d.length() == 1) {
            d = "0" + d;
        }
        return y + m + d;
    }

    private Table findTable(String contents, String docUrl) {

        StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
        selector.setKeywords(new String[]{"보유주식", "보유비율"});
        TableParser tableParser = new TableParser(selector);
        return (Table) tableParser.parse(contents, docUrl);
    }

    private String[] findBeforeNowDate(List<Map<String, Object>> maps) {
        String[] strings = new String[2];
        strings[0] = findBeforeDate(maps);
        strings[1] = findNowDate(maps);

        return strings;
    }

    private String findBeforeDate(List<Map<String, Object>> maps) {
        return findDate(maps, "직전보고서");
    }

    private String findNowDate(List<Map<String, Object>> maps) {
        return findDate(maps, "이번보고서");
    }

    private String findDate(List<Map<String, Object>> maps, String type) {
        for (Map<String, Object> map : maps) {
            if (map.toString().contains(type)) {
                return Maps.findValueAndKeys(map, "기준일");
            }
        }

        return "";
    }

    /**
     * 신규보고인데 실제로는 아닌 경우 확인
     * 이전 거래가 없으면 false
     * 이전 거래가 있는데 마지막 거래액이 0 이면 false
     * 나머지 true (fake신규보고)
     *
     * @param change
     * @return
     */
    private boolean checkFakeNewReport(Change change) {
        log.info("checkFakeNewReport " + change);
        String stockType = change.getStockType();
        String etc = change.getEtc();
        if (isEmpty(stockType) == false && stockType.contains("신규보고")) {
            if (isEmpty(etc) == false && etc.contains("기존보유")) {
                log.info("신규보고, ETC 기존보유 형태.. " + change);
                return true;
            } else {
                // 이전 거래 찾기...
                Map<String, Object> map = templateMapper.findNewerStockHolder(change.getIsuCd(), change.getName(), new Timestamp(change.getDateTime()));
                if (map == null) {
                    // 이전 거래가 없으면 fake는 아님.
                    return false;
                } else {
                    log.debug("checkFakeNewReport " + map);
                    Long after = Maps.getLongValue(map, "after_amt");
                    if (after == 0) {
                        // 이전 거래가 있었지만 모두 처리한 상태이후 거래하는 거라 fake 아님..
                        return false;
                    }
                }
                log.info("신규보고, 이미 보유형태. " + change);
                return true;
            }
        }

        return false;
    }

    private void setupBirthYm(List<Change> changes) {
        for (Change change : changes) {
            String birthDay = change.getBirthDay();
            if (isBirthDay(birthDay)) {
                change.setBirthYm(toYYMM(birthDay));
            } else {
                change.setBirthYm(birthDay);
            }
        }
    }

    private int findGiveNtake(Map<String, Object> param) {
        return templateMapper.findGiveNtake(param);
    }

    /**
     * 증여, 수증 데이터 처리.
     *
     * @param change
     */
    private void handleGiveNTake(Change change, String docNm) {
        if (StringUtils.containsAny(change.getStockType(), "증여", "수증")) {
            Map<String, Object> _param = findParam(change);
            if (findGiveNtake(_param) == 0) {
                Map<String, Object> param = new HashMap<>();
                param.putAll(_param);
                insertParam(change, param);
                templateMapper.insertGiveNTake(param);
                if (isGoodArticle(docNm)) {
                    sendToArticleQueue(rabbitTemplate, findKongsiKey(change), "GIVETAKE", _param);
                }
            } else {
                log.info("SKIP 수증증여 이미존재함 " + change);
            }
        }
    }

    private String findKongsiKey(Change change) {
        return change.getDocNo() + "_" + change.getIsuCd() + "_" + change.getAcptNo();
    }

    private Map<String, Object> findParam(Change change) {
        Map<String, Object> param = new HashMap<>();
        param.put("isu_cd", change.getIsuCd());
        param.put("change_date", modifyDate(change.getDateTime()));
        param.put("name", change.getName());
        param.put("change_amount", change.getChangeAmount() + "");
        param.put("before_amount", change.getBefore() + "");
        String stockType = change.getStockType();
        if (stockType.contains("증여")) {
            if (stockType.contains("취소")) {
                param.put("give_take_type", "GC");
            } else {
                param.put("give_take_type", "G");
            }

        } else if (stockType.contains("수증")) {
            if (stockType.contains("취소")) {
                param.put("give_take_type", "TC");
            } else {
                param.put("give_take_type", "T");
            }
        } else {
            param.put("give_take_type", "NG");
        }
        param.put("src_type", change.getSrcType());
        return param;
    }


    private void insertParam(Change change, Map<String, Object> param) {
        param.put("birth", change.getBirthDay());
        param.put("price", change.getPrice());
        param.put("price2", change.getPrice2());
        param.put("before_amount", change.getBefore());
        param.put("after_amount", change.getAfter());
        param.put("stock_type", change.getStockType());
        param.put("stock_method", change.getStockMethod());
        param.put("etc", change.getEtc());
        param.put("target", change.getTarget());
        param.put("target_type", change.getTargetType());
        param.put("doc_no", change.getDocNo());
        param.put("acpt_no", change.getAcptNo());
        param.put("acpt_dt", change.getAcptNo().substring(0, 8));
        param.put("gender", change.getGender());
        param.put("nationality", change.getNationality());
        param.put("relation", change.getRelation());


    }

    private String modifyDate(Long date) {
        return new SimpleDateFormat("yyyyMMdd").format(new Date(date));
    }

    /**
     * 대표이름으로 변경함.
     * 이것은 뷰에서 조회시에도 사용되는데...
     * 국민연금기금으로 조회를 하면 국민연금으로 실제로 조회를 하는 효과를 낸다.
     *
     * @param changes
     */
    private void setupRepresentativeName(List<Change> changes) {
        for (Change change : changes) {
            if (StringUtils.isEmpty(change.getName())) {
                continue;
            }
            modifyRepresentativeName(change);
        }
    }

    private String nvl(String s) {
        if (s == null) {
            return "";
        } else {
            return s;
        }
    }

    /**
     * 증여, 수증 데이터의 증여자,수증자정보가 있는 비고 정보를 보정함.
     * 1. chagnes 리스트에 증여, 수증 정보가 모두 있는 경우에만 해당
     * 2. 비고란에 증여자,수증자 정보가 없는 경우에만 해당.
     * 3. 증여, 수증 정보인 데이터에서 증감이 동일한 데이터에서 증여자와 수여자 정보를 찾을 수 있다.
     *
     * @param changes
     */
    private void setupBistowal(List<Change> changes) {
        List<Change> list = findPlusMinusChanges(changes);
        List<String> names = findNames(list);
        if (list.size() > 0) {
            for (Change change : list) {
                String etc = change.getEtc();
                etc = nvl(etc);
                // 비고란에 잘못된 데이터..
//                if (hasName(etc, names)==false) {
                log.debug("증여, 수증 비고의 다른 패턴임.. " + etc);
                String type = change.getStockType();
                Long changeAmount = change.getChangeAmount();
                Change c = null;
                if (type.contains("증여")) {
                    c = findMinusChange(changes, changeAmount, "수증");
                } else {
                    c = findMinusChange(changes, changeAmount, "증여");
                }


                if (c != null) {
                    log.debug(etc + " " + c.getName());
                    change.setEtc2(c.getName());
                    change.setTarget(c.getName());
                    setupTargetType(change, c);

                } else {
                    log.warn("수증 정보를 찾지 못함. " + change);
                }

                setupSrcType(change);

                if (isEmpty(change.getEtc2())) {
                    change.setEtc2(etc);
                }
                log.info(etc + " ==> " + change.getEtc2());
            }
        }
    }

    private void setupSrcType(Change change) {
        if (StringUtils.containsAny(change.getStockType(), "증여", "수증")) {
            // 증여,수증 데이터이면.. 주체의 type은 항상 정할 수 있다.
            String srcType = findSrcType(change.getBirthDay(), change.getName(), change.getIsuCd(), change.getAcptNo());
            change.setSrcType(srcType);

        }
    }

    private String findSrcType(String birth,String name,String code,String acptNo) {
        if (isEmpty(birth) || birth.contains("*") || "-".equals(birth)) {
            birth = "";
        }

        if (isRelative(name, birth, code)) {
            return "RELATIVE";
        } else {
            String _birth = BizUtils.convertBirth(birth, acptNo.substring(0, 8));
            if (isStaff(name, _birth, code)) {
                return "STAFF";
            } else {
                if (isCompany(code, name)) {
                    return "COMPANY";
                }else {
                    if (name.length() <= 3) {
                        if (findDashCount(birth) >= 2) {
                            return "COMPANY";
                        } else {
                            return "UNDEFINED";
                        }
                    } else {
                        if (isHuman(birth)) {
                            return "UNDEFINED";
                        } else {

                            return "COMPANY";
                        }
                    }
                }
            }
        }
    }

    private boolean isHuman(String s) {
        if (s.length() == 6) {
            // 생년월일
            return true;
        } else {
            if (findDashCount(s) == 1) {
                String[] strings = BizUtils.findNumeric(s);
                if (strings.length == 2) {
                    if (strings[0].length() == 6 && strings[1].length() == 7) {
                        // 주민번호인경우.
                        return true;
                    }
                }
            }
        }

        return false;
    }
    private int findDashCount(String s) {
        int count = 0;
        for (char c : s.toCharArray()) {
            if (c == '-') {
                count++;
            }
        }
        return count;
    }
    private boolean isCompany(String code, String name) {
        Map<String, Object> param = new HashMap<>();
        param.put("name", name);
        param.put("isu_cd", code);
        Map<String,Object> map = templateMapper.findLargests(param);
        if (map == null) {
            return false;
        }

        String gender = Maps.getValue(map, "gender");
        if (StringUtils.containsAny(gender, "남", "여", "남자", "여자")) {
            return false;
        } else {
            return true;
        }

    }
    private boolean isStaff(String name, String birth, String isuCd) {
        Map<String, Object> param = new HashMap<>();
        param.put("name", name);
        param.put("birth", birth);
        param.put("isu_cd", isuCd);
        if (templateMapper.findStaffCount(param) > 0) {
            return true;
        } else {
            return false;
        }
    }
    private void setupOneTaker(List<Change> list, Change oneGiver) {
        for (Change change : list) {
            String type = change.getStockType();
            if (type.contains("수증")) {
                oneGiver.setEtc2(change.getName());
                oneGiver.setTarget(change.getName());
                setupTargetType(oneGiver, change);
            }
        }
    }

    private void setupOneGiver(List<Change> list, Change oneGiver) {
        for (Change change : list) {
            String type = change.getStockType();
            if (type.contains("수증")) {
                change.setEtc2(oneGiver.getName());
                change.setTarget(oneGiver.getName());
                setupTargetType(change, oneGiver);
            }
        }
    }

    private Change findOneCancelGiver(List<Change> list) {
        for (Change change : list) {
            String stockType = change.getStockType();
            if ("증여취소(+)".equalsIgnoreCase(stockType)) {
                return change;
            }
        }

        return null;
    }

    private boolean hasOneCancelGiver(List<Change> list) {
        int count = 0;
        for (Change change : list) {
            String stockType = change.getStockType();
            if ("증여취소(+)".equalsIgnoreCase(stockType)) {
                count++;
            }
        }

        if (count == 1) {
            return true;
        } else {
            return false;
        }
    }

    private void setupTargetType(Change change, Change c) {
        if (isCompany(c)) {
            log.info("company " + c);
            change.setTargetType("COMPANY");
        } else {
            if (isRelative(c)) {
                log.info("RELATIVE " + c);
                change.setTargetType("RELATIVE");
            } else {
                log.info("STAFF " + c);
                change.setTargetType("STAFF");
            }
        }
    }

    private Change findOneGiver(List<Change> list) {
        for (Change change : list) {
            String stockType = change.getStockType();
            if ("증여(-)".equalsIgnoreCase(stockType)) {
                return change;
            }
        }

        return null;
    }

    private boolean hasOneGive(List<Change> list) {
        int count = 0;
        for (Change change : list) {
            String stockType = change.getStockType();
            if ("증여(-)".equalsIgnoreCase(stockType)) {
                count++;
            }
        }

        if (count == 1) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isRelative(String isuCd,String name,String birth) {
        Map<String, Object> param = new HashMap<>();
        param.put("name", name);
        param.put("birth", birth);
        param.put("isu_cd", isuCd);
        if (templateMapper.findRelativeHodlerSize(param) > 0) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isRelative(Change target) {
        return isRelative(target.getIsuCd(), target.getName(), target.getBirthDay());
    }

    private boolean isCompany(Change change) {
        String birth = change.getBirthDay();
        if (birth.length() == 6 && StringUtils.isNumeric(birth)) {
            return false;
        } else {
            if (birth.contains("*")) {
                return false;
            } else {
                return true;
            }
        }
    }

    private Change findMinusChange(List<Change> changes, Long changeAmount, String type) {
        for (Change change : changes) {
            log.debug(change + " << " + changeAmount + " << " + type);
            if (change.getStockType().contains(type)) {
                if (Math.abs(change.getChangeAmount()) == Math.abs(changeAmount)) {
                    return change;
                }
            }
        }
        return null;
    }

    private boolean hasName(String etc, List<String> names) {
        for (String name : names) {
            if (etc.contains(name)) {
                return true;
            }
        }
        return false;
    }

    private List<String> findNames(List<Change> list) {
        List<String> strings = new ArrayList<>();
        for (Change change : list) {
            strings.add(change.getName());
        }

        return strings;
    }

    private List<Change> findPlusMinusChanges(List<Change> changes) {
        List<Change> changes1 = new ArrayList<>();
        for (Change change : changes) {
            String stockType = change.getStockType();
            if (StringUtils.containsAny(stockType, "증여(-)", "수증(+)", "증여취소(+)", "수증취소(-)")) {
                changes1.add(change);
            }
        }
        return changes1;
    }


    private boolean hasEmptyValue(Change change) {
        if (change.getBefore() == 0 && change.getAfter() == 0) {
            return true;
        } else {
            return false;
        }
    }

    private boolean validate(String raw) {
        raw = raw.trim();
        if (raw.endsWith("</HTML>")) {
            return true;
        } else if (raw.endsWith("</html>")) {
            return true;
        } else {
            return false;
        }
    }

    private int countList(List<Long> longs) {
        if (longs == null) {
            return 0;
        } else {
            return longs.size();
        }
    }

    /**
     * 최대주주등소유주식변동신고서 스크래핑한 데이터중에 처리일시가 잘못되어 중복되어 나왔는데 이걸 방지하기 위한 로직임.
     * 두개의 제한 조건이 들어감
     * 1. 동일한 공시에서는 처리일시까지 넣어 중복 데이터를 찾는다.
     * 2. 다른 공시에서의 처리
     * - 처리일시가 같으면 단가는 빼고 중복데이터틑 찾는다.
     * - 처리일시가 다르면 단가는 넣고 중복데이터를 찾는다.
     *
     * @param templateMapper
     * @param param
     * @return
     */
    private Long hasDuplicateStockHolder(TemplateMapper templateMapper, Map<String, Object> param) {

        List<Long> seqs = templateMapper.findDupStockSeqOnSameKind(param);
        int count = countList(seqs);
        if (count > 0) {
            Long l = seqs.get(0);
            log.info("SEQ1 " + l + " " + count);
            param.put("stock_seq", l);
            // 중북된 공시는 stockdupholder insert함
            handleDupStock(templateMapper, param);
            return l;
        } else {
            seqs = templateMapper.findDupStockSeqOnOtherKind(param);
            count = countList(seqs);
            if (count > 0) {
                Long l = seqs.get(0);
                log.info("SEQ2 " + l + " " + count);
                param.put("stock_seq", l);
                // 중북된 공시는 stockdupholder insert함
                handleDupStock(templateMapper, param);
                return l;
            } else {
                return null;
            }
        }


    }

    /**
     * 거래내약 중복된 공시 정보를 수집함.
     * 중복되지 않은 공시만 입력함.
     *
     * @param templateMapper
     * @param param
     */
    private void handleDupStock(TemplateMapper templateMapper, Map<String, Object> param) {
        if (templateMapper.findDupStockCount(param) == 0) {
            templateMapper.insertDupStock(param);
        }
    }

    private void updateStockHolderBirthDay(TemplateMapper templateMapper, Map<String, Object> map) {
        templateMapper.updateStockHolderBirthDay(map);
    }

    private Map<String, Object> toParam(Change change, String code) {
        Map<String, Object> map = BizUtils.changeParamMap(change, code);
        map.put("birth_day", change.getBirthDay2());
        map.put("birth_day2", change.getBirthDay());

        return map;
    }

    private boolean isBirthdayUpdate(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__UPDATE")) {
            if ("BIRTHDAY".equalsIgnoreCase((String) message.getMessageProperties().getHeaders().get("__UPDATE"))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 퇴임한 임원 정보 생성.
     *
     * @param change
     * @return
     */
    private Map<String, Object> createParam(Change change) {
        String name = change.getName();
        String kongsiDate = DateUtils.toString(change.getDateType(), "yyyyMMdd");
        String birth = BizUtils.convertBirth(change.getBirthDay(), kongsiDate);
        String code = change.getIsuCd();
        String docNo = change.getDocNo();
        String acptNo = change.getAcptNo();
        String docUrl = change.getDocUrl();
        return createParam(name, birth, kongsiDate, code, docNo, acptNo, docUrl);
    }

    private Map<String, Object> createParam(String name, String birth, String kongsiDate, String code, String docNo, String acptNo, String docUrl) {
        Map<String, Object> param = new HashMap<>();
        param.put("kongsi_day", kongsiDate);
        param.put("name", name);
        param.put("birth_day", birth);
        param.put("isu_cd", code);
        param.put("doc_no", docNo);
        param.put("acpt_no", acptNo);
        param.put("doc_url", docUrl);
        param.put("spot", "임원퇴임");
        return param;
    }

    private void modifyRepresentativeName(Change change) {
        String ownerName = change.getName().trim();
        if (myContext.hasRepresentativeName(ownerName)) {
            String _ownerName = myContext.findRepresentativeName(ownerName);
            log.info(ownerName + " ==> " + _ownerName);
            change.setName(_ownerName);

        }
    }

    private void handleExpiration(TemplateMapper templateMapper, Map<String, Object> param) {
        int count = templateMapper.findExpirationCount(param);
        if (count == 0) {
            templateMapper.insertExpiration(param);
        }
    }

    /**
     * 임원퇴임 거래 처리..
     * @param templateMapper
     * @param param
     */
    private void handleExpiration2(TemplateMapper templateMapper, Map<String, Object> param) {

        int _count = templateMapper.existStaff(param);
        if (_count > 0) {
            // 임원인지 여부 먼저 확인.
            int count = templateMapper.findStaff(param);
            if (count == 0) {
                log.info("staffholder <<< " + param);
                templateMapper.insertStaff(param);
            } else {
                log.info("SKIP 중복된 임원 " + param);
            }
        } else {
            log.info("SKIP 임원이 아님. "+param);
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

    private boolean hasExpiration2(Change change) {
        String stockType = change.getStockType();
        if (StringUtils.contains(stockType, "임원퇴임")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * change 객체에 보정되지 않은 단가는 unitPrice2에 있음.
     * unitPrice는 보정된 단가로 이 메소드에서 설정함.
     * 단가가 0이면 해당일의 종가를 가져와 설정함.
     *
     * @param changes
     * @param codeNm
     */
    private void setupPrice(List<Change> changes, String codeNm) {
        for (Change change : changes) {
            if (change.getPrice() == 0) {
                log.debug("  " + change.getDate());
                Integer price = null;
                if (BizUtils.isPreferredStock(change.getStockMethod())) {
                    // 우선주 회사코드 필요
                    String preferredCode = BizUtils.findPreferredCode(change.getIsuCd());
                    price = templateMapper.findClose(preferredCode, new Timestamp(change.getDateTime()));
                } else {
                    price = templateMapper.findClose(change.getIsuCd(), new Timestamp(change.getDateTime()));
                }
                if (price == null || price == 0) {
                    log.debug("no close .. " + change.getDate() + " " + change.getIsuCd());
                } else {
                    log.debug("0 ==>  " + price);
                    change.setPrice(price);
                    change.setupProfit();
                }
            }
        }
    }

//    private String findPreferredCode(String code) {
//        return code.substring(0, code.length() - 1) + "5";
//    }


//    /**
//     * TODO 변경가능.
//     * 우선주인지 여부 확인
//     *
//     * @param change
//     * @return
//     */
//    private boolean isPreferredStock(Change change) {
//        String stockMethod = change.getStockMethod();
//        if (StringUtils.equalsAny(stockMethod, "종류주식", "우선주", "자동전환우선주")) {
//            return true;
//        } else {
//            return false;
//        }
//    }

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

    private Map<String, Object> findBirthDay(String docRaw, String docUrl) throws Exception {
        StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
        selector.setKeywords(new String[]{"보고자", "관한", "사항"});
        TableParser stockChangeTableParser = new TableParser(selector);
        return (Map<String, Object>) stockChangeTableParser.simpleParse(docRaw, docUrl, new StaffInfoParser());
    }

    private long insertStockHolder(TemplateMapper templateMapper, Map<String, Object> map) {
        return templateMapper.insertStockHolder(map);
    }

    private void deleteBeforeStockHolder(TemplateMapper templateMapper, String _docNo, String code) {
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
        int count = templateMapper.findStockHolderCount(param);
        if (count > 0) {
            return true;
        } else {

            return false;
        }
    }


}
