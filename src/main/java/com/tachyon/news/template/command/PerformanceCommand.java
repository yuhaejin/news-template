package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.KrxCrawlerHelper;
import com.tachyon.crawl.kind.model.Estate;
import com.tachyon.crawl.kind.model.Performance;
import com.tachyon.crawl.kind.parser.EstateMyParser;
import com.tachyon.crawl.kind.parser.MyParser;
import com.tachyon.crawl.kind.parser.PerformanceMyParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.FinancialStatementSelectorByPattern;
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;

/**
 * 실적 처리
 * 분기,반기,사업보고서에서 손익계산서 부분을 분석하여
 * 연결재무제표, 재무제표의
 * 수익(매출액)
 * 영업이익(손실)
 * 당기순이익(손실)
 * 데이터를 저장함.
 * <p>
 * 반기나 사업보고서 데이터가 분기데이터가 아닌 경우가 있는데
 * 이런 데이터는 분기데이터는 보정하고 있다.
 */
@Slf4j
@Component
public class PerformanceCommand extends BasicCommand {
    private String[] PERIODS = {"1Q", "2Q", "1SA", "3Q", "3QA", "4Q", "2SA", "1Y"};
    //코스피200 금융회사들.. (매출액을 다르게 분석해야 해서..)
    private String[] KOSPI200_BANKS = {"A105560", "A055550", "A086790", "A316140", "A024110", "A071050", "A138930","A006800","A016360","A039490","A005940","A008560"};
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

            if (isPeriodDataCorrect(message)) {
                correctBizPerf(message);

            } else {
                handleBizPerf(message);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

    }



    @Override
    public String findArticleType() {
        return "ESTIMATE";
    }

    /**
     * 분기데이터 보정처리..
     * @param message
     */
    private void correctBizPerf(Message message) {
        //                반기나 년기 데이터를 통해 분기데이터 보정한다.
        // seq (반기,년기 데이터)
        // isucd와 seq조회 데이터로  나머지를 구할 수 있다.
        // 분기데이터가 만들어지면 INSERT를 수행.
        String seq = myContext.findInputValue(message);
        log.info("실적보정 " + seq);
        if (isEmpty(seq)) {
            log.error("실적 기간데이터 보정을 할 수 없음. seq 값이 없음 ");
            return;
        }
        Map<String, Object> map = templateMapper.findBizPerf(Long.valueOf(seq));
        if (map == null) {
            log.error("실적데이터가 없음. " + seq);
            return;
        }

        String code = Maps.getValue(map, "isu_cd");
        String period = Maps.getValue(map, "period");
        String periodType = Maps.getValue(map, "period_type");
        if (period.startsWith("ERROR")) {
            log.error("에러가 있는 기간데이터 " + period + " " + seq);
            return;
        }

        String year = period.substring(0, 4);
        List<Map<String, Object>> mapList = templateMapper.findBizPerfs(code, year);
        sortBizPerf(mapList);
        for (Map<String, Object> _map : mapList) {
            log.info("정렬후 " + _map.toString());
        }

        //TODO 기사 생성할 수 있도록 추후 변경.
        if (isFirstBangi(period)) {
            handleFirstBangi(mapList, code, year);
        } else if (isSecondBangi(period)) {
            // 이런 경우는 없음.
            handleSecondBangi(mapList, code, year);
        } else if (isYear(period)) {
            handleYear(mapList, code, year);
        } else {
            if ("QANG".equals(periodType)) {
                handle3QA(mapList, code, year);
            } else {
                log.info("처리하지 못하는 기간패턴 " + period + " " + seq);
            }
        }

    }

    /**
     * 년기 데이터 보정.
     * 4분기 실적 데이터 처리.
     *
     * @param mapList
     */
    private void handleYear(List<Map<String, Object>> mapList, String code, String year) {
        // 4분기 데이터가 있는지 확인. 있으면 SKIP
        // 1,2,3분기 데이터 있는지 확인 없으면 SKIP
        // 년기 데이터 있는지 확인 없으면 SKIP
        // 4분기 데이터 생성
        // 중복 확인
        // 있으면
        handleYearBangi2(mapList, code, year, "1Y");
    }

    /**
     * 2반기 실적 데이터 보정.
     * 4분기 실적 데이터 처리.
     *
     * @param mapList
     */
    private void handleSecondBangi(List<Map<String, Object>> mapList, String code, String year) {
        // 4분기 데이터가 있는지 확인. 있으면 SKIP
        // 1,2,3분기 데이터 있는지 확인 없으면 SKIP
        // 2반기 데이터 있는지 확인 없으면 SKIP
        // 4분기 데이터 생성
        // 중복 확인
        // 있으면
        handleYearBangi(mapList, code, year, "2SA");


    }

    /**
     * 3분기 누적데이터만 있는 경우.
     * @param mapList
     * @param code
     * @param year
     */
    private void handle3QA(List<Map<String, Object>> mapList, String code, String year) {
        String key = "3QA";
        Map<String, Object> map3Q = findBungi(mapList, "3Q", year);
        if (map3Q != null) {
            log.info("NO 3분기 데이터가 이미 있음. " + code + " " + year);
//            return;
        } else {
            log.debug("OK 3분기데이터 미존재");
        }

        List<Map<String, Object>> maps = findBungis(mapList, year, "1Q", "2Q");
        if (maps == null || maps.size() != 2) {
            log.info("NO 1,2분기 데이터가 모두 있지 않음. " + code + " " + year);
            return;
        } else {
            log.debug("OK 1,2분기 데이터 존재" + maps);
        }
        // 3분기 누적 데이터
        Map<String, Object> map = findBungi(mapList, key, year);
        if (map == null) {
            log.info("NO " + key + " 데이터가 없음. " + code + " " + year);
            return;
        } else {
            log.debug("OK " + key + " 데이터 존재" + map);
        }

        String period = year + "/3Q";
        Map<String, Object> param3Q = make3QData(maps, map);

        log.debug("생성된 3분기 데이터 " + param3Q);
        if (map3Q == null) {
            log.info("INSERT BIZPERF " + param3Q);
            insertBizPerf(param3Q);
        } else {
            param3Q.put("seq", Maps.getLongValue(map3Q, "seq"));
            updateBizPerf(param3Q);
            log.info("UPDATE BIZPERF " + param3Q);

        }
//        if (findPeriodCount(code, year + "/4Q") == 0) {
//
//        } else {
//            log.info("4분기데이터 이미 존재. " + code + " " + period);
//        }
    }

    private Map<String, Object> make3QData(List<Map<String, Object>> maps, Map<String, Object> map) {
        Map<String, Object> map3Q = new HashMap<>();
        map3Q.putAll(map);
        setupRevenue(map3Q, maps, map);
        setupCrevenue(map3Q, maps, map);
        setupOperatingProfit(map3Q, maps, map);
        setupCoperatingProfit(map3Q, maps, map);
        setupNetIncome(map3Q, maps, map);
        setupCnetIncome(map3Q, maps, map);
//        setupCumulatedRevenue(map3Q, map);

        setupBungiPeriod(map3Q, "3Q");
        if (hasError(map3Q)) {
            return null;
        }
        return map3Q;
    }

    /**
     * 4분기누적 - (1분기+2분기+3분기)
     * @param mapList
     * @param code
     * @param year
     * @param key
     */
    private void handleYearBangi(List<Map<String, Object>> mapList, String code, String year, String key) {
        Map<String, Object> map4Q = findBungi(mapList, "4Q", year);
        if (map4Q != null) {
            log.info("NO 4분기 데이터가 이미 있음. " + code + " " + year);
//            return;
        } else {
            log.debug("OK 4분기데이터 미존재");
        }
        List<Map<String, Object>> maps = findBungis(mapList, year, "1Q", "2Q", "3Q");
        if (maps == null || maps.size() != 3) {
            log.info("NO 1,2,3분기 데이터가 모두 있지 않음. " + code + " " + year);
            return;
        } else {
            log.debug("OK 1,2,3분기 데이터 존재" + maps);
        }
        String period = year + "/4Q";

        /**
         * 연기데이터 추출
         */
        Map<String, Object> map = findBungi(mapList, key, year);
        if (map == null) {
            log.info("NO " + key + " 데이터가 없음. " + code + " " + year);
            return;
        } else {
            log.debug("OK " + key + " 데이터 존재" + map);
        }

        Map<String, Object> param4Q = make4QData(maps, map);

        log.debug("생성된 4분기 데이터 " + param4Q);
        if (map4Q == null) {
            log.info("INSERT BIZPERF " + param4Q);
            insertBizPerf(param4Q);
        } else {
            param4Q.put("seq", Maps.getLongValue(map4Q, "seq"));
            updateBizPerf(param4Q);
            log.info("UPDATE BIZPERF " + param4Q);

        }
//        if (findPeriodCount(code, year + "/4Q") == 0) {
//
//        } else {
//            log.info("4분기데이터 이미 존재. " + code + " " + period);
//        }
    }

    /**
     * 4분기누적 - 3분기누적 으로 4분기 데이터 생성.
     *
     * 3분기 누적이 없으면 handleYearBangi 으로 처리하게 하자.
     * @param mapList
     * @param code
     * @param year
     * @param key
     */
    private void handleYearBangi2(List<Map<String, Object>> mapList, String code, String year, String key) {
        Map<String, Object> map4Q = findBungi(mapList, "4Q", year);
        if (map4Q != null) {
            log.info("NO 4분기 데이터가 이미 있음. " + code + " " + year);
//            return;
        } else {
            log.debug("OK 4분기데이터 미존재");
        }
        List<Map<String, Object>> maps = findBungis(mapList, year, "3Q");
        if (maps == null || maps.size() != 1) {
            log.info("NO 3분기 데이터가 모두 있지 않음. " + code + " " + year);
            return;
        } else {
            log.debug("OK 3분기 데이터 존재" + maps);
        }

        // 3분기 누적 데이터 확인
        // 있으면 OK 없으면 handleYearBangi 호출..
        Map<String, Object> map3Q = maps.get(0);
        String cumulatedRevenue = Maps.getValue(map3Q, "cum_revenue");
        String cumulatedCrevenue = Maps.getValue(map3Q, "cum_crevenue");
        if (isEmpty(cumulatedRevenue) && isEmpty(cumulatedCrevenue)) {
            handleYearBangi(mapList, code, year, key);

        } else {
            String period = year + "/4Q";
            /**
             * 연기데이터 추출
             */
            Map<String, Object> yearMap = findBungi(mapList, key, year);
            if (yearMap == null) {
                log.info("NO " + key + " 데이터가 없음. " + code + " " + year);
                return;
            } else {
                log.debug("OK " + key + " 데이터 존재" + yearMap);
            }

            Map<String, Object> param4Q = make4QDataWithCumulated(maps, yearMap);

            log.debug("생성된 4분기 데이터 " + param4Q);
            if (map4Q == null) {
                log.info("INSERT BIZPERF " + param4Q);
                insertBizPerf(param4Q);
            } else {
                param4Q.put("seq", Maps.getLongValue(map4Q, "seq"));
                updateBizPerf(param4Q);
                log.info("UPDATE BIZPERF " + param4Q);

            }

        }

    }




    private List<Map<String, Object>> findBungis(List<Map<String, Object>> mapList, String year, String... keys) {
        List<Map<String, Object>> maps = new ArrayList<>();
        for (String key : keys) {
            Map<String, Object> map = findBungi(mapList, key, year);
            if (map != null) {
                maps.add(map);
            }
        }

        return maps;
    }

    /**
     *
     * @param maps 1,2,3Q 분기 데이터터     * @param map
     * @return
     */
    private Map<String, Object> make4QData(List<Map<String, Object>> maps, Map<String, Object> map) {

        Map<String, Object> map4Q = new HashMap<>();
        map4Q.putAll(map);
        setupRevenue(map4Q, maps, map);
        setupCrevenue(map4Q, maps, map);
        setupOperatingProfit(map4Q, maps, map);
        setupCoperatingProfit(map4Q, maps, map);
        setupNetIncome(map4Q, maps, map);
        setupCnetIncome(map4Q, maps, map);

        setupBungiPeriod(map4Q, "4Q");
        return map4Q;
    }

    /**
     * TODO 4분기 누적 - 3분기누적처리
     * @param map3Q 3Q 누적데이터를 가짐.
     * @param yearMap
     * @return
     */
    private Map<String, Object> make4QDataWithCumulated(List<Map<String, Object>> map3Q, Map<String, Object> yearMap) {
        Map<String, Object> map4Q = new HashMap<>();
        map4Q.putAll(yearMap);
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_revenue","revenue");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_operating_profit","operating_profit");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_net_income","net_income");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_crevenue","crevenue");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_coperating_profit","coperating_profit");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_cnet_income","cnet_income");
        setupBungiPeriod(map4Q, "4Q");
        return map4Q;
    }


    private boolean hasError(Map<String, Object> map) {
        return map.containsKey("ERROR");
    }

    private void setupRevenue(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2) {
        setupValue(target, srcs, src2, "revenue");
    }

    private void setupCrevenue(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2) {
        setupValue(target, srcs, src2, "crevenue");
    }

    private void setupOperatingProfit(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2) {
        setupValue(target, srcs, src2, "operating_profit");
    }

    private void setupCoperatingProfit(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2) {
        setupValue(target, srcs, src2, "coperating_profit");
    }

    private void setupNetIncome(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2) {
        setupValue(target, srcs, src2, "net_income");
    }

    private void setupCnetIncome(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2) {
        setupValue(target, srcs, src2, "cnet_income");
    }

    private void setupValue(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2, String key) {
        String value = findValue(srcs, src2, key);
        if (value == null) {

        } else {
            target.put(key, value);
        }

    }

    /**
     * 누적에서 값을 찾아 일반키에 저장하여 키가 두개가 필요함.
     * @param target 최종 결과값
     * @param srcs 이전 분기데이터
     * @param src2 마지막 분기데이터
     * @param key 분기데이터에서 찾을 값의 키
     * @param key2 최종결과로 저장할 키
     */
    private void setupCumulatedValue(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2, String key,String key2) {
        String value = findValue(srcs, src2, key);
        if (value == null) {

        } else {
            target.put(key2, value);
        }

    }

    private String findValue(List<Map<String, Object>> srcs, Map<String, Object> src2, String key) {
        boolean isSameUnit = isSameUnit(srcs, src2);
        String value2 = Maps.getValue(src2, key);
        if (isEmpty(value2)) {
            //년기나 3개월누적 데이터가 없으면 데이터보정이 의미가 없으므로.
            return "";
        }
        if (isSameUnit) {
            long l2 = findMoney(value2);
            long l1 = findMoney(srcs, key);

            if (l1 == -1) {
                //1,2,3분기 데이터중에 하나가 없으면 데이터보정이 의미가 없으므로.
                return "";
            }
            long v = l2 - l1;
            log.debug(key + "값보정 " + v + " = " + l2 + " - " + l1);
            if (v == 0) {
                return "";
            } else {
                return BizUtils.toMoney(v);
            }
        } else {
            //TODO
            return null;
        }
    }
    private boolean isSameUnit(List<Map<String, Object>> srcs, Map<String, Object> src2) {
        String _unit = Maps.getValue(src2, "unit");
        for (Map<String, Object> map : srcs) {
            String unit = Maps.getValue(map, "unit");
            if (StringUtils.equalsIgnoreCase(_unit, unit) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * 1반기 실적 데이터 보정.
     * 2분기 실적 데이터 생성처리.
     *
     * @param mapList
     */
    private void handleFirstBangi(List<Map<String, Object>> mapList, String code, String year) {

        // 2분기 데이터가 있는지 확인. 있으면 SKIP
        // 1분기 데이터 있는지 확인 없으면 SKIP
        // 1반기 데이터 있는지 확인 없으면 SKIP
        // 2분기 데이터 생성
        // 중복 확인
        // 있으면
        Map<String, Object> map2Q = findBungi(mapList, "2Q", year);
        if (map2Q != null) {
            log.info("NO 2분기 데이터가 이미 있음. " + code + " " + year);
//            return;
        } else {
            log.debug("OK 2분기데이터 미존재");
        }
        Map<String, Object> map1Q = findBungi(mapList, "1Q", year);
        if (map1Q == null) {
            log.info("NO 1분기 데이터가 없음. " + code + " " + year);
            return;
        } else {
            log.debug("OK 1분기 데이터 존재" + map1Q);
        }
        Map<String, Object> map1SA = findBungi(mapList, "1SA", year);
        if (map1SA == null) {
            log.info("NO 1반기 데이터가 없음. " + code + " " + year);
            return;
        } else {
            log.debug("OK 1반기 데이터 존재" + map1SA);
        }
        String period = year + "/2Q";
        Map<String, Object> param2Q = make2QData(map1Q, map1SA);

        log.debug("생성된 2분기 데이터 " + param2Q);
        if (map2Q == null) {
            log.info("INSERT BIZPERF " + param2Q);
            insertBizPerf(param2Q);
        } else {
            param2Q.put("seq", Maps.getLongValue(map2Q, "seq"));
            updateBizPerf(param2Q);
            log.info("UPDATE BIZPERF " + param2Q);
        }

    }

    private void updateBizPerf(Map<String, Object> param2Q) {
        templateMapper.updateBizPerf(param2Q);
    }

    private int findPeriodCount(String code, String period) {
        return templateMapper.findPeriodCount(code, period);
    }

    private void insertBizPerf(Map<String, Object> param) {
        templateMapper.insertBizPerfHolder(param);
    }

    /**
     * 반기데이터에서 분기데이터 생성.
     *
     * @param map1Q
     * @param map1SA
     * @return
     */
    private Map<String, Object> make2QData(Map<String, Object> map1Q, Map<String, Object> map1SA) {
        //반기에서 1분기데이터 빼기
        // 다른 메타데이터는 반기데이터와 동일하게 처리.

        Map<String, Object> map2Q = new HashMap<>();
        map2Q.putAll(map1SA);
        setupRevenue(map2Q, map1Q, map1SA);
        setupCrevenue(map2Q, map1Q, map1SA);
        setupOperatingProfit(map2Q, map1Q, map1SA);
        setupCoperatingProfit(map2Q, map1Q, map1SA);
        setupNetIncome(map2Q, map1Q, map1SA);
        setupCnetIncome(map2Q, map1Q, map1SA);
        setupBungiPeriod(map2Q, "2Q");
        return map2Q;
    }

    /**
     * period와 periodtype를 설정함.
     *
     * @param map2Q
     */
    private void setupBungiPeriod(Map<String, Object> map2Q, String type) {
        String period = Maps.getValue(map2Q, "period");
        String year = period.substring(0, 4);
        map2Q.put("period", year + "/" + type);
        map2Q.put("period_type", "BUCR");   //BUNGI_CREATE
    }

    private void setupValue(Map<String, Object> target, Map<String, Object> src1, Map<String, Object> src2, String key) {
        String unitKey = findUnitKey(key);
        String value2 = Maps.getValue(src2, key);
        String unit2 = Maps.getValue(src2, unitKey);
        String value1 = Maps.getValue(src1, key);
        if (isEmpty(value2)) {
            String period = Maps.getValue(src2, "period");
            String isuCd = Maps.getValue(src2, "isu_cd");
            log.error(isuCd+" "+period+" "+ key+"에 해당하는 값이 없음.");
            target.put(key, "");
            return;
        }

        if (isEmpty(value1)) {
            String period = Maps.getValue(src1, "period");
            String isuCd = Maps.getValue(src1, "isu_cd");
            log.error(isuCd+" "+period+" "+ key+"에 해당하는 값이 없음.");
            target.put(key, "");
            return;
        }
        String unit1 = Maps.getValue(src1, unitKey);
        if (StringUtils.equalsIgnoreCase(unit1, unit2)) {
            long l2 = findMoney(value2);
            long l1 = findMoney(value1);
            long v = l2 - l1;
            if (v == 0) {
                target.put(key, "");
            } else {
                target.put(key, BizUtils.toMoney(v));
            }
        } else {
            // 이거는 나중에 해보자...
        }
    }

    private String findUnitKey(String key) {
        if (key.toLowerCase().startsWith("c")) {
            return "unit2";
        } else {
            return "unit";
        }
    }


    private long findMoney(String s) {
        return BizUtils.findMoney(s);
    }

    private long findMoney(List<Map<String, Object>> srcs, String key) {
        long sum = 0;
        for (Map<String, Object> map : srcs) {
            String v = Maps.getValue(map, key);
            if (isEmpty(v)) {
                String period = Maps.getValue(map, "period");
                String isuCd = Maps.getValue(map, "isu_cd");
                log.error(isuCd+" "+period+" "+key+"에 해당하는 값이 없음. ");
                return -1;
            }
            sum += findMoney(v);
            log.debug(key + " sum=" + sum + " << " + v);
        }
        return sum;
    }

    private void setupCoperatingProfit(Map<String, Object> map2Q, Map<String, Object> map1Q, Map<String, Object> map1SA) {
        setupValue(map2Q, map1Q, map1SA, "coperating_profit");
    }

    private void setupOperatingProfit(Map<String, Object> map2Q, Map<String, Object> map1Q, Map<String, Object> map1SA) {
        setupValue(map2Q, map1Q, map1SA, "operating_profit");
    }

    private void setupCrevenue(Map<String, Object> map2Q, Map<String, Object> map1Q, Map<String, Object> map1SA) {
        setupValue(map2Q, map1Q, map1SA, "crevenue");
    }

    private void setupRevenue(Map<String, Object> map2Q, Map<String, Object> map1Q, Map<String, Object> map1SA) {
        setupValue(map2Q, map1Q, map1SA, "revenue");
    }

    private void setupCnetIncome(Map<String, Object> map2Q, Map<String, Object> map1Q, Map<String, Object> map1SA) {
        setupValue(map2Q, map1Q, map1SA, "cnet_income");
    }

    private void setupNetIncome(Map<String, Object> map2Q, Map<String, Object> map1Q, Map<String, Object> map1SA) {
        setupValue(map2Q, map1Q, map1SA, "net_income");
    }

    private Map<String, Object> findBungi(List<Map<String, Object>> mapList, String type, String year) {
        for (Map<String, Object> map : mapList) {
            String period = Maps.getValue(map, "period");
            if (StringUtils.equalsIgnoreCase(period, year + "/" + type)) {
                return map;
            }
        }
        return null;
    }

    private boolean isYear(String period) {
        return period.contains("1Y");
    }

    private boolean isSecondBangi(String period) {
        return period.contains("2SA");
    }

    private boolean isFirstBangi(String period) {
        return period.contains("1SA");
    }

    private int findPeriodIndex(String period) {
        for (int i = 0; i < PERIODS.length; i++) {
            String _period = PERIODS[i];
            if (period.contains(_period)) {
                return i;
            }
        }

        return 100;
    }

    private void sortBizPerf(List<Map<String, Object>> mapList) {

        Collections.sort(mapList, new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                String period1 = Maps.getValue(o1, "period");
                period1 = StringUtils.remove(period1, "_");
                String period2 = Maps.getValue(o2, "period");
                period2 = StringUtils.remove(period2, "_");

                int i1 = findPeriodIndex(period1);
                int i2 = findPeriodIndex(period2);

                return Integer.valueOf(i1).compareTo(Integer.valueOf(i2));
            }
        });
    }

    /**
     * 기본적인 공시 분석처리.
     * @param message
     * @throws Exception
     */
    private void handleBizPerf(Message message) throws Exception {
        String key = myContext.findInputValue(message);
        log.info("<<< " + key);

        boolean updateCorrect = isBizPerfCorrect(message);
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

        // 보고서 손익계산서 분석처리..
        FinancialStatementSelectorByPattern selector = new FinancialStatementSelectorByPattern();
        PerformanceMyParser myParser = new PerformanceMyParser();
        EstateMyParser estateMyParser = new EstateMyParser();
        TableParser tableParser = new TableParser(selector);

        // 연결재무제표 분석..
        Map<String, Object> map = parseCfs(selector, tableParser, c, docUrl, myParser);
        // 개별재무제표 분석..
        Map<String, Object> map2 = parseFs(selector, tableParser, c, docUrl, myParser);
        if (map == null && map2 == null) {
            log.error("연결재무제표, 재무제표 모두 없음. " + docUrl + " " + key);
            // 분석한 대상이 없더라도 최종 결과는 db에 처리하므로 여기에서 멈추지 않는다.
        }

        log.debug("연결재무재표 분석 결과 "+map);
        log.debug("개별재무재표 분석 결과 "+map2);

        if (isKospi200Banks(code)) {
            // ## 금융회사는 방안이 정해지지 않아서 아래 로직은 모든 경우를 처리하지는 못함.
            // 코스피200 금융회사인 경우는 매출액을 따로 분석한다.
            // fnguide 데이터 형식으로 처리함에 유의바람.
            if (map != null) {
                //연결재무재표가 있는 경우.
                log.info("코스피200 금융회사 연결매출액 분석.. "+isuNm);
                selector.setKeywords(new String[]{"연결", "재무", "주석"});
                selector.setNoKeywords(new String[]{});
                Map<String, Object> estateMap = findEstateMap(c,docUrl,tableParser,estateMyParser);

                log.info("연계분석결과 "+map);
                log.info("연계주석분석결과 "+estateMap);
                Estate estate = Estate.fromConnect(map, estateMap);
                log.info("연결매출액결과 "+estate);
                if (estate.validate()) {
                    //정상인 경우에만 정상적인 값을 부여한다.
                    estate.setType("연결");
                    map.put("CTAKE", estate.getTake());
                }
                // 분석된 json은 정상적인 분석여부와 상관없이 db처리함.
                map.put("cbank_json", JsonUtils.toJson(estate));

            }

            if (map2 != null) {
                //개별재무재표가 있는 경우.
                selector.setKeywords(new String[]{"재무", "주석"});
                selector.setNoKeywords(new String[]{"연결"});
                Map<String, Object> estateMap = findEstateMap(c,docUrl,tableParser,estateMyParser);

                Estate estate = Estate.from(map, estateMap);
                map.put("TAKE", estate.findTake());
                map.put("bank_json", JsonUtils.toJson(estate));

            }

        }



        // 단위가 원화가 아닌 경우 요약재무정보에서 가져오게 처리해야 함.
        String unit = "";
        if (map != null) {
            unit = Maps.getValue(map, "UNIT");
        }
        String unit2 = "";
        if (map2 != null) {
            unit2 = Maps.getValue(map2, "UNIT");
        }

        boolean isSummary = false;
        if (BizUtils.isNotWon(unit) || BizUtils.isNotWon(unit2)) {
            if (isKospi200Banks(code)==false) {
                // 연결이나 개별 중에 하나라도 다른 통화를 사용하면 연결요약과 개별요약 모두 분석한다.
                isSummary = true;
                selector.setKeywords(new String[]{"요약", "재무"});
                selector.setNoKeywords(new String[]{});

                myParser.setSummrayCase(true);
                myParser.setIndividual(false);
                log.debug("연결원화재분석 이전 " + map);
                map = (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);
                log.debug("연결원화재분석 이후 " + map);

                myParser.setSummrayCase(true);
                myParser.setIndividual(true);
                log.debug("개별원화재분석 이전 " + map2);
                map2 = (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);
                log.debug("개별원화재분석 이후 " + map2);
            }

        }

        Map<String, Object> params = findParams(map, map2, acptNo, docNm, key,isSummary);
        params.put("doc_no", docNo);
        params.put("isu_cd", code);
        params.put("acpt_no", acptNo);
        params.put("acpt_dt", acptNo.substring(0, 8));
        if (isKospi200Banks(code)) {
            if (map != null) {
                //코스피200 금융조목은 매출액 처리를 하지 않는다. (헛갈릴수 있음)
                params.put("crevenue", "");
                // 정상적이지 않은 분석도 분석결과는 저장.
                params.put("cbank_json", Maps.getValue(map, "cbank_json"));
            }
            if (map2 != null) {
                //코스피200 금융조목은 매출액 처리를 하지 않는다. (헛갈릴수 있음)
                params.put("revenue", "");
                // 정상적이지 않은 분석도 분석결과는 저장.
                params.put("bank_json", Maps.getValue(map, "bank_json"));
            }
        }

        setupPeriod(params, key);
        // 정정공시 처리
        if (docNm.contains("정정")) {
            List<String> _docNos = findBeforeKongsi(templateMapper,docNo, code, acptNo);
            for (String _docNo : _docNos) {
                log.info("정정공시중에 이전 공시 삭제... " + _docNo + " " + code);
                templateMapper.deleteBeforeBizPerfHolder(_docNo, code);
                deleteBeforeArticle(templateMapper, _docNo, code, acptNo,findArticleType());
            }
        }

        Long seq = templateMapper.findBizPerfHolder(params);
        String period = Maps.getValue(params, "period");
        String subPk = code + "_" + period;
        if (seq == null) {
            templateMapper.insertBizPerfHolder(params);
            seq = Maps.getLongValue(params, "seq");
            log.info("INSERT 실적 " + params);
            if (isGoodArticle(docNm)) {
                sendToArticleQueue(rabbitTemplate,seq+"",findArticleType(),subPk);
            }
        } else {
            if (updateCorrect) {
                params.put("seq", seq);
                templateMapper.updateBizPerf(params);
                log.info("UPDATE 실적 " + params);
                sendToArticleQueue(rabbitTemplate,seq+"",findArticleType(),subPk,true);
            } else {
                log.info("이미 존재.. SKIP.. " + params);
            }
        }

        // 반기나 년기인 경우 보정처리를 시도해 본다.
        String periodType = Maps.getValue(params, "period_type");
        if (StringUtils.equalsAny(periodType, "YUNG", "BANG", "QANG")) {
            sendToCorrectQueue(rabbitTemplate, seq + "", periodType);
        }
    }

    private Map<String, Object> findEstateMap(String c,String docUrl,TableParser tableParser, EstateMyParser estateMyParser) throws Exception {
        return (Map<String, Object>) tableParser.simpleParse(c, docUrl, estateMyParser);
    }

    private boolean isKospi200Banks(String code) {
        for (String s : KOSPI200_BANKS) {
            if (s.equalsIgnoreCase(code)) {
                return true;
            }
        }
        return false;
    }

    private void sendToCorrectQueue(RabbitTemplate rabbitTemplate, String seq, String periodType) {
        log.info("_BIZ_PERF 보정처리 <<< " + seq + " " + periodType);
        rabbitTemplate.convertAndSend("_BIZ_PERF", (Object) seq, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                message.getMessageProperties().setPriority(9);
                message.getMessageProperties().getHeaders().put("__CORRECT", "YES");
                return message;
            }
        });
    }


    /**
     * 반기나 년기 데이터를 통해 분기데이터 보정을 하는지..
     *
     * @param message
     * @return
     */
    private boolean isPeriodDataCorrect(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__CORRECT")) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isBizPerfCorrect(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__RECORRECT")) {
            return true;
        } else {
            return false;
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
            params.put("period_type", "ERRR");
        } else {
            if (period.contains("ERROR")) {
                params.put("period", "ERROR_PARSING_" + key);
                params.put("period_type", "ERRR");
            } else {
                if (period.contains("QA")) {
                    params.put("period_type", "QANG");
                } else if (period.contains("Q")) {
                    params.put("period_type", "BUNG");
                } else if (period.contains("Y")) {
                    params.put("period_type", "YUNG");
                } else if (period.contains("SA")) {
                    params.put("period_type", "BANG");
                }
            }
        }
    }

    private Map<String, Object> findSubPk() {
        return null;
    }

    private String findPk() {
        return null;
    }

    private Map<String, Object> findParams(Map<String, Object> map, Map<String, Object> map2, String acptNo, String docNm, String key,boolean isSummary) {
        Map<String, Object> params = new HashMap<>();
        // 두개의 재무제표를 모두 분석하지 못함..
        if (map == null && map2 == null) {
            return params;
        }

        if (map != null) {
            Performance performance = BizUtils.findPerformance(map,isSummary);
            log.info("연결재무제표실적 " + performance);
            performance.setFromData("연결재무제표");
            performance.setDocNm(docNm);
            performance.setKey(key);
            // 연결재무제표인 경우에..
            performance.setUnit2(performance.getUnit());
            performance.setAcptUrl(KrxCrawlerHelper.getItemUrl(acptNo));
            if (hasThreeMonthInKey(performance)) {
                String period = performance.getPeriod();
                String _docNm = findDocNm(docNm);
                log.info("3개월 PERIOD분석 " + performance.getPeriod() + " " + _docNm);
                performance.setParsedPeriod(BizUtils.findPeriod(period, _docNm, true));
            } else {
                log.info("PERIOD분석 " + performance.getPeriod() + " " + docNm);
                performance.setParsedPeriod(BizUtils.findPeriod(performance.getPeriod(), docNm));
            }
            performance.param(params);
        }

        if (map2 != null) {
            Performance performance = BizUtils.findPerformance(map2,isSummary);
            log.info("재무제표실적 " + performance);
            performance.setFromData("재무제표");
            performance.setDocNm(docNm);
            performance.setKey(key);
            performance.setAcptUrl(KrxCrawlerHelper.getItemUrl(acptNo));
            if (hasThreeMonthInKey(performance)) {
                String period = performance.getPeriod();
                String _docNm = findDocNm(docNm);
                log.info("3개월 PERIOD분석 " + performance.getPeriod() + " " + _docNm);
                performance.setParsedPeriod(BizUtils.findPeriod(period, _docNm, true));
            } else {
                log.info("PERIOD분석 " + performance.getPeriod() + " " + docNm);
                performance.setParsedPeriod(BizUtils.findPeriod(performance.getPeriod(), docNm));
            }

            performance.param(params);


        }

        return params;
    }

    private String findDocNm(String src) {
        if (src.contains("사업보고서")) {
            return StringUtils.replace(src, "사업보고서", "분기보고서");
        } else if (src.contains("반기보고서")) {
            return StringUtils.replace(src, "반기보고서", "분기보고서");
        } else {
            return src;
        }
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
