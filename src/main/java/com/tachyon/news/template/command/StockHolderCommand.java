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
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
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
            String docRaw = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, loadBalancerCommandHelper);
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
                    //birthday가 없을 수도 있음
                    Table _table = findBirthDay(docRaw, docUrl);
                    String birthDay = _table.findBirthDay();
                    String birthDay2 = _table.findBirthDay("BIRTH_DAY2");

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
                            String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
                            log.info("이전StockHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);
                            if (StringUtils.isEmpty(_docNo) == false) {
                                if (docNo.equalsIgnoreCase(_docNo) == false) {
                                    deleteBeforeStockHolder(templateMapper, code, _docNo);
                                    log.info("이전StockHolder 삭제 code=" + code + " docNo=" + _docNo);
                                    DELETE.put(_docNo, _docNo);
                                }
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
                        continue;
                    }
                    if (hasEmptyValue(change)) {
                        continue;
                    }

                    changes.add(change);
                }
            }

            log.info("주식거래내역 ... " + changes.size());

            setupName(changes, myContext);
            setupPrice(changes, codeNm);
            setupRepresentativeName(changes);
            setupBistowal(changes);
            setupBirthYm(changes);
            int stockCount = 0;

            boolean isCompressedData = findCompressedData(changes,rptNm,docUrl,docRaw);
            if (isCompressedData) {
                log.info("압축된 StockData " + docUrl);
            } else {
                log.info("압축된 StockData가 아님. "+docUrl);
            }
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
                        if (findStockHolder(templateMapper, code, change)) {
                            Map<String, Object> _map = toParam(change, code);
                            if (StringUtils.equals(change.getBirthDay(), change.getBirthDay2()) == false) {
                                log.info("updateStockHolderBirthDay " + _map);
                                updateStockHolderBirthDay(templateMapper, _map);
                            } else {
                                log.info("SKIP 생일파싱이 동일함 " + _map);
                            }
                        }
                    } else {
                        // 유일한 값이라고 할만한 조회...
                        //mybatis 처리시 paramMap을 다른 클래스에서 처리한 것은 나중에 수정..
                        Map<String, Object> findParam = param(change, code, tempRptNm, docUrl, docNo, acptNo);
                        Long seq = hasDuplicateStockHolder(templateMapper, findParam);
                        if (seq!=null) {
                            // 정정된 것은 이미 삭제가 되었으므로 업데이트하지 않아도 딱히 문제가 없음.
                            log.info("ALREADY StockHodler " + change);
                            if (isCompressedData) {
                                log.info("Compress StockData "+seq);
                                compressStockHolder(templateMapper,seq);
                            }

                        } else {
                            Map<String, Object> param = change.paramStockHolder(code, acptNo);
                            setupCompressed(param, isCompressedData);
                            setupYesterDayClosePrice(param,change);

                            log.info("INSERT ... " + param);
                            insertStockHolder(templateMapper, param);
                            modifyParam(findParam);
                            if (change.getChangeAmount() != 0) {
                                // 변동량이 있을 때만 기사 작성하기.. by sokhoon 20200605
                                sendToArticleQueue(rabbitTemplate,findPk(param),"STOCK",findParam);
                            }
                            stockCount++;
                        }

                        if (hasExpiration2(change)) {
                            log.info("임기만료 ... " + change);
                            handleExpiration2(templateMapper, createParam(change));
                        }
                    }

                    handleGiveNTake(change);

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


    private void modifyParam(Map<String, Object> findParam) {
        findParam.remove("doc_url");
        findParam.remove("acpt_no");
        findParam.remove("doc_no");
        findParam.remove("isu_cd");
    }







    private Map<String,Object> param(Change change,String code,String tempRptNm,String docUrl,String docNo,String acptNo) {
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
    private void setupYesterDayClosePrice(Map<String, Object> param,Change change) {
        param.put("odd_value_yn", "N");
        String _code = change.getIsuCd();
        String stockMethod = change.getStockMethod();
        if (BizUtils.isSkipStockMethod(stockMethod)) {
            log.debug("SKIP 처리하지 않아도 될 주식 주말이어서 처리하지 않음 "+change);
            return;
        }

        long price = change.getPrice();
        Date changeDate = new Date(change.getDateTime());
        if (price == 0) {
            // price가 0 인데 오늘이면 SKIP
            if (DateUtils.isToday(changeDate)) {
                log.debug("SKIP 오늘은 종가가 없어 처리하지 않음 "+change);
                return;
            }
            // 거래일이 주말이면 SKIP...
            if (DateUtils.isWeekend(changeDate)) {
                log.debug("SKIP 거래일이 주말이어서 처리하지 않음 "+change);
                return;
            }
        }

        Date yesterDay = findYesterDay(changeDate);
        Timestamp _yesterDay = new Timestamp(yesterDay.getTime());

        String code = BizUtils.findCode(_code, stockMethod);
        Integer closePrice  = templateMapper.findCloseByStringDate(code, _yesterDay);
        if (closePrice == null) {
            log.warn("종가를 찾을 수 없음. "+code+" "+_yesterDay);
            return;
        }
        long value = Math.abs(price - closePrice);
        int percentage = percentage(value, closePrice);
        if (percentage >= 30) {
            param.put("odd_value_yn", "Y");
            param.put("before_price", closePrice+"");
            String name = change.getName();
            log.warn("전날종가보다 +-30% 임 종목명=" + code + " 변동일=" + change.getDate() +" "+name+ " 전날=" + DateUtils.toString(yesterDay,"yyyy-MM-dd") + " 전날종가=" + closePrice + " 현단가="+price+ " percentage=" + percentage+" "+change.getDocUrl());
            //TODO 텔레그램 알림..
        } else {
            log.debug("전날종가보다 +=30% 이하임..");
        }
    }



    /**
     *  전날을 구한다.
     *  다만 웡요일이면 금요일을 구해야 함.
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
        Calendar cal = Calendar.getInstance() ;
        cal.setTime(date);
        int dayNum = cal.get(Calendar.DAY_OF_WEEK) ;
        if (dayNum == 2) {
            return true;
        } else {
            return false;
        }
    }

    private int percentage(double value, double closePrice) {
        return (int)(value * 100 / closePrice);
    }
    private String modifyDate(long dateTime) {
        return DateUtils.toString(new Date(dateTime), "yyyyMMdd");
    }

    /**
     * 거래내역이 단건이 아닌 여러건을 한번에 압축하여 처리한 것인지 표기한다.
     * @param templateMapper
     * @param seq
     */
    private void compressStockHolder(TemplateMapper templateMapper, Long seq) {
        templateMapper.compressStockHolder(seq);
    }

    /**
     * 압축된 여부를 적용.
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
     * @param changes
     * @param rptNm
     * @param docUrl
     * @param contents
     * @return
     */
    private boolean findCompressedData(List<Change> changes, String rptNm,String docUrl,String contents ) {
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
                        int count = templateMapper.findBeforeStock(code,name,beforeDate, nowDate);
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
        return DateUtils.toString(new Date(dateTime),"yyyyMMdd");
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
        return  (Table) tableParser.parse(contents, docUrl);
    }

    private String[] findBeforeNowDate(List<Map<String,Object>> maps) {
        String[] strings = new String[2];
        strings[0] = findBeforeDate(maps);
        strings[1] = findNowDate(maps);

        return strings;
    }

    private String findBeforeDate(List<Map<String,Object>> maps) {
        return findDate(maps, "직전보고서");
    }

    private String findNowDate(List<Map<String,Object>> maps) {
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
        if (isEmpty(stockType)==false && stockType.contains("신규보고")) {
            if (isEmpty(etc)==false && etc.contains("기존보유")) {
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
    private void handleGiveNTake(Change change) {
        if (StringUtils.containsAny(change.getStockType(), "증여", "수증")) {
            Map<String, Object> _param = findParam(change);
            if (findGiveNtake(_param) == 0) {
                Map<String, Object> param = new HashMap<>();
                param.putAll(_param);
                insertParam(change, param);
                templateMapper.insertGiveNTake(param);
                sendToArticleQueue(rabbitTemplate,findKongsiKey(change),"GIVETAKE",_param);
            } else {
                log.info("SKIP 수증증여 이미존재함 " + change);
            }
        }
    }

    private String findKongsiKey(Change change) {
        return change.getDocNo()+"_"+change.getIsuCd()+"_"+change.getAcptNo();
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
//            if (hasOneGive(list)) {
//                Change oneGiver = findOneGiver(list);
//                setupOneGiver(list, oneGiver);
//                if (list.size() == 2) {
//                    setupOneTaker(list, oneGiver);
//                }
//
//            } else if (hasOneCancelGiver(list)) {
//                Change oneGiver = findOneCancelGiver(list);
//                setupOneGiver(list, oneGiver);
//                if (list.size() == 2) {
//                    setupOneTaker(list, oneGiver);
//                }
//            } else {
//
//            }


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

//                }else {
//                    log.debug("증여, 수증 비고의 기본 패턴임.. "+etc);
//                }
                // etc2의 값이 없다고 하면 etc로 설정함.
                if (isEmpty(change.getEtc2())) {
                    change.setEtc2(etc);

                }

                log.info(etc + " ==> " + change.getEtc2());
            }
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

    private boolean isRelative(Change target) {
        String isuCd = target.getIsuCd();
        String name = target.getName();
        String birth = target.getBirthDay();
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
    private Long hasDuplicateStockHolder(TemplateMapper templateMapper, Map<String,Object> param) {

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
     * @param change
     * @return
     */
    private Map<String, Object> createParam(Change change) {
        String name = change.getName();
        String kongsiDate = DateUtils.toString(change.getDateType(), "yyyyMMdd");
        String birth = BizUtils.convertBirth(change.getBirthDay(),kongsiDate);
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

    private void handleExpiration2(TemplateMapper templateMapper, Map<String, Object> param) {
        int count = templateMapper.findStaff(param);
        if (count == 0) {
            log.info("staffholder <<< " + param);
            templateMapper.insertStaff(param);
        } else {
            log.info("SKIP 중복된 임원 " + param);
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
                    log.debug("no...");
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

    private Table findBirthDay(String docRaw, String docUrl) throws Exception {
        StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
        selector.setKeywords(new String[]{"보고자", "관한", "사항"});
        TableParser stockChangeTableParser = new TableParser(selector);
        List<Table> tables = stockChangeTableParser.parseSome(docRaw, docUrl, new StaffBirthDayParser());
        if (tables == null || tables.size() == 0) {
            return null;
        } else {
            Table table = tables.get(0);
            return table;
        }
    }

    private long insertStockHolder(TemplateMapper templateMapper, Map<String, Object> map) {
        return templateMapper.insertStockHolder(map);
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
