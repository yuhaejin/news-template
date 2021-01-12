package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.MyTakingParser;
import com.tachyon.crawl.kind.parser.MyTakingParser2;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.TakingSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.Tables;
import com.tachyon.crawl.kind.util.Utils;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.Borrowing;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.model.Taking;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.tachyon.crawl.BizUtils.modifyWithUnit;

/**
 * 차입금 템플릿 처리
 * 일단 주식등의 대량보유상황보고서 공시에서 추출하여 분석함.
 */
@Slf4j
@Component
public class TakingHolderCommand extends BasicCommand {
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

        if (isOldAtCorrectedKongsi(map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }
        // DB 확인...
//        int count = templateMapper.findTakingCount(docNo, code, acptNo);
//        if (count > 0) {
//            log.info("SkIP :  이미처리됨. " + key + " " + docUrl);
//            return;
//        }

        if (docNm.contains("주식등의대량보유상황보고서") == false) {
            log.info("SkIP :  공시명 " + docNm + " " + key + " " + docUrl);
            return;
        }

        String html = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl,retryTemplate, loadBalancerCommandHelper);
        if (isEmpty(html)) {
            log.error("공시Html을 수집할 수 없음. " + key + " " + docUrl);
            return;
        }
        TakingSelectorByPattern selector = new TakingSelectorByPattern();
        TableParser parser = new TableParser(selector);
        List<Table> tables = parser.parseSome(html, docUrl, new MyTakingParser());
        List<Map<String, Object>> takings = new ArrayList<>();
        if (tables != null) {
            for (Table t : tables) {
                if ("차임금".equalsIgnoreCase(t.getTitle())) {
                    takings.add(Tables.widTableToMap(t, false));
                } else {
                    List<Map<String, Object>> maps = t.toMapList();
                    for (Map<String, Object> _map : maps) {
                        _map.put("UNIT", t.getUnit());
                        takings.add(_map);
                    }
                }
            }
        }

        if (takings.size() == 0) {
            log.info("취득데이터가 없음.  " + key + " " + docUrl);
            return;
        }

        for (Map<String, Object> _map : takings) {
            log.info("<<< " + _map);
        }
        Taking _taking = findReporter(selector, html, docUrl);
        log.info("... " + docUrl);
        Map<String, Taking> takingMap = aggregate(takings, key, docUrl);
        handleBeforeTaking(code, acptNo, docNo);
        for (String name : takingMap.keySet()) {
            Taking taking = takingMap.get(name);
            String birth = taking.getBirth();
            if (_taking != null) {
                if (name.equalsIgnoreCase(_taking.getName()) && birth.equalsIgnoreCase(_taking.getBirth())) {
                    taking.setSpot(_taking.getSpot());
                }
            }
            String result = taking.validate(key);
            if ("OK".equalsIgnoreCase(result) == false) {
                taking.setType("ERROR");
                taking.setErrorMsg(result);
                log.warn("INVALID " + result + " " + taking + " " + docUrl);
            } else {
                taking.setErrorMsg("");
            }
            //회사코드_성명_생년월일_자기자금_차입금_기타
            List<Borrowing> borrowings = taking.getBorrowings();
            if (borrowings != null) {
                for (Borrowing borrowing : borrowings) {
                    Map<String, Object> findParam = findParam(taking, borrowing, code);
                    log.info("FIND_PARAM "+findParam);
                    if (fincTakingHolderCount(findParam) == 0) {
                        Map<String, Object> insertParam = makeParam(taking,borrowing, docNo, code, acptNo);
                        insertTakingHolder(insertParam);
                        if (isGoodArticle(docNm)) {
                            sendToArticleQueue(rabbitTemplate,findPk(insertParam),"TAKING",findParam);
                        }
                    } else {
                        log.info("SKIP 이미존재하는 취득 " + taking+" "+borrowing);
                    }
                }
            }
        }
    }

    private Map<String, Object> findParam(Taking taking, Borrowing borrowing, String code) {
        Map<String, Object> map = new HashMap<>();
        String unit = taking.getUnit();
        map.put("isu_cd", code);
        map.put("name", taking.getName());
        map.put("birth", taking.getBirth());
        map.put("owner_capital", modifyWithUnit(taking.getTaking(),unit));
        map.put("borrow_amount", modifyWithUnit(borrowing.getBorrowingAmount(),unit));
        map.put("etc", taking.getEtc());
        return map;
    }


    private Taking findReporter(TakingSelectorByPattern selector, String html, String docUrl) throws Exception {
        selector.setKeywrods(new String[]{"대량보유자", "사항"});
        TableParser parser = new TableParser(selector);
        List<Table> tables = parser.parseSome(html, docUrl, new MyTakingParser2());
        for (Table table : tables) {
            if ("보고자개요".equalsIgnoreCase(table.getTitle())) {
                Map<String, Object> map = Tables.widTableToMap(table);
                Taking taking = new Taking();
                taking.setSpot(Maps.findValue(map, "직업"));
                taking.setName(Maps.findValue(map, "성명(명칭)"));
                taking.setBirth(Maps.findValue(map, "생년월일"));
                return taking;
            }
        }

        return null;
    }

    private void insertTakingHolder(Map<String,Object> param ) {
        log.info("INSERT " + param);
        templateMapper.insertTakingHolder(param);
    }

    private Map<String, Object> makeParam(Taking taking, Borrowing borrowing,String docNo, String code, String acptNo) {
        Map<String, Object> map = new HashMap<>();
        String unit = taking.getUnit();
        map.put("name", taking.getName());
        map.put("birth", taking.getBirth());
        map.put("owner_capital", modifyWithUnit(taking.getTaking(),unit));
        map.put("borrowings", modifyWithUnit(taking.getBorrowing(),unit));
        map.put("etc", modifyWithUnit(taking.getEtc(),unit));
        map.put("sum", modifyWithUnit(taking.getSum(),unit));
        map.put("resource", StringUtils.abbreviate(taking.getResource(), 200));
        map.put("borrower", borrowing.getBorrower());
        map.put("borrow_amount", modifyWithUnit(borrowing.getBorrowingAmount(),borrowing.getUnit()));
        map.put("borrow_period", borrowing.getBorrowingPeriod());
        map.put("collateral", borrowing.getCollateral());
        map.put("doc_no", docNo);
        map.put("isu_cd", code);
        map.put("acpt_no", acptNo);
        map.put("acpt_dt", acptNo.substring(0, 8));
        map.put("type", taking.getType());
        map.put("spot", taking.getSpot());
        map.put("err_msg", StringUtils.abbreviate(taking.getErrorMsg(), 100));
        return map;
    }

    private int fincTakingHolderCount(Taking taking,Borrowing borrowing, String code) {
        int count = templateMapper.fincTakingHolderCount(code, taking.getName(), taking.getBirth(), taking.getTaking(), borrowing.getBorrowingAmount(), taking.getEtc());
        log.info("TAKINGCOUNT " + count + " " + taking.getName());
        return count;
    }
    private int fincTakingHolderCount(Map<String,Object> param) {
        int count = templateMapper.fincTakingHolderCount(param);
        return count;
    }

    private void handleBeforeTaking(String code, String acptNo, String docNo) {
        String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
        log.info("이전TaskingHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);

        if (StringUtils.isEmpty(_docNo) == false) {
            if (docNo.equalsIgnoreCase(_docNo) == false) {
                deleteBeforeTakingHolder(templateMapper, code, _docNo);
                log.info("이전TaskingHolder 삭제 code=" + code + " docNo=" + _docNo);
                deleteBeforeArticle(templateMapper,_docNo,acptNo,code);
            }
        }
    }

    private void deleteBeforeTakingHolder(TemplateMapper templateMapper, String code, String docNo) {
        templateMapper.deleteBeforeTakingHolder(code, docNo);
    }

    /**
     * 취득 테이블의 데이터를 적당히 합친다.
     * 일반취득자금데이터에 자기자금, 차입금 데이터를 끼워넣음.
     *
     * @param takings
     * @return
     */
    private Map<String, Taking> aggregate(List<Map<String, Object>> takings, String key, String docUrl) {
        Map<String, Taking> map = new LinkedHashMap<>();
        findGeneral(map, takings, key, docUrl);
        aggregate(map, takings, key);
        return map;
    }

    /**
     * 취득자금등의 개요 테이블 분석
     *
     * @param map
     * @param takings
     */
    private void findGeneral(Map<String, Taking> map, List<Map<String, Object>> takings, String key, String docUrl) {
        for (Map<String, Object> _map : takings) {
            if ("개요".equalsIgnoreCase(findType(_map))) {
                log.info(">>>> " + _map + " " + key + " " + docUrl);
                Taking taking = toTaking(_map);
                if (taking != null) {
                    log.debug("<<< " + taking);
                    taking.setType("개요");
                    if ("-".equalsIgnoreCase(taking.getName())) {
                        log.info("SKIP 이름이없음. " + taking);
                    } else {
                        map.put(taking.findKey(), taking);
                    }
                }
            }
        }
    }

//    private String removeSpace(String s) {
//        return StringUtils.remove(s, " ");
//    }

    private String modifyName(String s) {
        s = StringUtils.remove(s, " ");
        return Utils.removeBracketBrace(s);
    }

    /**
     * 취득자금등의 개요 테이블이 아닌 테이블 정보 분석
     *
     * @param map
     * @param takings
     */
    private void aggregate(Map<String, Taking> map, List<Map<String, Object>> takings, String key) {
        for (Map<String, Object> _map : takings) {
            String type = findType(_map);
            if ("개요".equalsIgnoreCase(type)) {
                continue;
            }
            log.info(">> " + _map);
            if ("자기자금".equalsIgnoreCase(type)) {
                String name = findName(_map);
                String birth = findBirth(_map);
                String resource = findResource(_map);
                Taking taking = findTaking(map, modifyName(name), modifyName(birth));
                if (taking != null) {
                    taking.setType(Taking.MY);
                    taking.setResource(resource);
                    taking.setupGneral(Taking.MY);
                } else {
                    log.warn("개요정보를 찾을 수 없음. " + name + " " + birth + " " + key);
                }

            } else if ("차입금".equalsIgnoreCase(type)) {
                String name = Utils.removeBracketBrace(Maps.getValue(_map, "차입자"));
                if ("0".equalsIgnoreCase(name)) {
                    continue;
                }

                String[] names = StringUtils.split(name, ",");
                String borrower = Maps.getValue(_map, "차입처");
                String borrowingPeriod = Maps.getValue(_map, "차입기간");
                String collateral = Maps.getValue(_map, "담보내역");
                String unit = Maps.getValue(_map, "UNIT");
                if (names.length == 1) {
                    String borrowingAmount = adjustPrice((Maps.getValue(_map, "차입금액")));
                    Taking taking = null;
                    if (map.size() == 1) {
                        taking = findOneTaking(map);
                    } else {
                        taking = findTaking(map, modifyName(name));
                    }
                    if (taking != null) {
                        taking.setType(Taking.BORROWING);
                        Borrowing borrowing = new Borrowing();
                        borrowing.setBorrower(borrower);
                        borrowing.setBorrowingPeriod(borrowingPeriod);
                        borrowing.setBorrowingAmount(borrowingAmount);
                        borrowing.setCollateral(collateral);
                        borrowing.setUnit(unit);

                        taking.addBorrowings(borrowing);
                        taking.setupGneral(Taking.BORROWING);
                    } else {
                        log.warn("개요정보를 찾을 수 없음. " + name + " " + key);
                    }
                } else {
                    for (String _name : names) {
                        Taking taking = findTaking(map, modifyName(_name));
                        if (taking != null) {
                            String borrowingAmount = adjustPrice((Maps.getValue(_map, "차입금액")));
                            taking.setType(Taking.BORROWING);
                            // 차입금액은 두명의 합친금액이므로 설정하지 않음.
                            Borrowing borrowing = new Borrowing();
                            borrowing.setBorrower(borrower);
                            borrowing.setBorrowingPeriod(borrowingPeriod);
                            borrowing.setBorrowingAmount(borrowingAmount);
                            borrowing.setCollateral(collateral);
                            borrowing.setUnit(unit);
                            taking.addBorrowings(borrowing);
                            taking.setupGneral(Taking.BORROWING);
                        } else {
                            log.warn("개요정보를 찾을 수 없음. " + name + " " + key);
                        }

                    }

                }

            } else {

            }

        }
    }


    private Taking findOneTaking(Map<String, Taking> map) {
        for (String key : map.keySet()) {
            Taking taking = map.get(key);
            return taking;
        }

        return null;
    }

    private Taking findTaking(Map<String, Taking> map, String name) {
        for (String key : map.keySet()) {
            if (key.equalsIgnoreCase(name)) {
                Taking taking = map.get(key);
                return taking;
            }
        }

        return null;
    }

    private Taking findTaking(Map<String, Taking> map, String name, String birth) {
        for (String key : map.keySet()) {
            if (key.equalsIgnoreCase(name)) {
                Taking taking = map.get(key);
                if (taking.getBirth().equalsIgnoreCase(birth)) {
                    return taking;
                }
            }
        }

        return null;
    }


    private String findResource(Map<String, Object> map) {
        return Maps.getValue(map, "취득자금등의조성경위및원천");
    }

    private String findName(Map<String, Object> _map) {
        return Maps.getValue(_map, "성명");
    }

    private String findBirth(Map<String, Object> _map) {
        return Maps.findValueOrKeys(_map, "생년월일");
    }

    private Taking toTaking(Map<String, Object> _map) {
        Taking taking = new Taking();
        String name = findName(_map);
        taking.setName(modifyName(name));
        taking.setBirth(removeSpace(findBirth(_map)));
        taking.setTaking(adjustPrice(Maps.findValueOrKeys(_map, "자기자금")));
        taking.setBorrowing(adjustPrice(Maps.findValueOrKeys(_map, "차입금")));
        taking.setEtc(adjustPrice(Maps.findValueOrKeys(_map, "기타")));
        taking.setSum(adjustPrice(Maps.findValueOrKeys(_map, "계")));
        taking.setUnit(Maps.getValue(_map, "UNIT"));
        taking.setupGeneral();
        taking.setupUnit();
        return taking;
    }

    private String removeSpace(String birth) {
        return StringUtils.remove(birth, " ");
    }

    private String adjustPrice(String s) {
        s = StringUtils.remove(s, ",");
        s = StringUtils.remove(s, "-");
        s = StringUtils.remove(s, "원");
        s = s.trim();
        if ("".equalsIgnoreCase(s)) {
            return "0";
        } else {
            return s;
        }
    }

    /**
     * 데이터 유형 확인
     * 일반,자기자금,차입금
     *
     * @param _map
     * @return
     */
    private String findType(Map<String, Object> _map) {

        if (_map.containsKey("성명") == false) {
            return "차입금";
        } else {
            if (Maps.hasContainsKey(_map, "취득자금등의조성경위및원천")) {
                return "자기자금";
            } else {
                return "개요";
            }
        }
    }

    /**
     * TODO 정정 이전 공시 삭제처리..
     *
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
