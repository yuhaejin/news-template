package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.KrxCrawlerHelper;
import com.tachyon.crawl.kind.model.Performance;
import com.tachyon.crawl.kind.parser.*;
import com.tachyon.crawl.kind.parser.handler.FinancialStatementSelectorByPattern;
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

import static com.tachyon.crawl.BizUtils.*;

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
    // 보험사
    private String[] INSURANCES = {"A000060", "A001450", "A000370", "A000540", "A000400", "A000810"};    //DB손해보험은 제외함.
    private String[] LIFE_INSURANCES = {"A032830", "A082640", "A088350", "A085620"};    //생명보험
    //증권사
    private String[] STOCKS = {"A005940", "A071050", "A016360", "A008560", "A039490", "A003460", "A030610", "A003530", "A003470", "A001500", "A001720", "A003540", "A001270", "A078020", "A016610", "A001510", "A001750", "A030210", "A001290", "A190650"};

    private String[] BANKS = addAll(INSURANCES, STOCKS, LIFE_INSURANCES);
    //    private String[] C_INSURANCE = {"000810"};  //삼성화재해상보험.
    @Autowired
    private MyContext myContext;

    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private RabbitTemplate rabbitTemplate;


    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    private static final String BANGI = "BANG";   //반기
    private static final String BUNGI = "BUNG";   //분기
    private static final String YUNGI = "YUNG";   //연기
    private static final String QANGI = "QANG";   //


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


    private String[] addAll(String[]... strings) {
        int size = 0;
        for (String[] strings1 : strings) {
            size += strings1.length;
        }

        String[] result = new String[size];
        int index = 0;
        for (String[] strings2 : strings) {
            for (String s : strings2) {
                result[index++] = s;
            }
        }
        return result;
    }

    /**
     * 분기데이터 보정처리..
     *
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

        if (isFirstBangi(period)) {
            handleFirstBangi(mapList, code, year);
        } else if (isSecondBangi(period)) {
            // 이런 경우는 없음.
            handleSecondBangi(mapList, code, year);
        } else if (isYear(period)) {
            handleYear(mapList, code, year);
        } else {
            if (QANGI.equals(periodType)) {
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
     *
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

        setupBungiPeriod(map3Q, "3Q");
        if (hasError(map3Q)) {
            return null;
        }
        return map3Q;
    }

    /**
     * 4분기누적 - (1분기+2분기+3분기)
     *
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
     * <p>
     * 3분기 누적이 없으면 handleYearBangi 으로 처리하게 하자.
     *
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
            log.info("3분기 누적데이터가 없어 1,2,3분기 데이터로 4분기 데이터 처리.");
            handleYearBangi(mapList, code, year, key);

        } else {
            log.info("3분기 누적데이터가 있어 년기 데이터와의 차이로 4분기 데이터 처리.");
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
     * @param maps 1,2,3Q 분기 데이터터
     * @param map  1Y 데이터
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
     * 4분기 누적 - 3분기누적처리
     *
     * @param map3Q   3Q 누적데이터를 가짐.
     * @param yearMap
     * @return
     */
    private Map<String, Object> make4QDataWithCumulated(List<Map<String, Object>> map3Q, Map<String, Object> yearMap) {
        Map<String, Object> map4Q = new HashMap<>();
        map4Q.putAll(yearMap);
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_revenue", "revenue");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_operating_profit", "operating_profit");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_net_income", "net_income");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_crevenue", "crevenue");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_coperating_profit", "coperating_profit");
        setupCumulatedValue(map4Q, map3Q, yearMap, "cum_cnet_income", "cnet_income");
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
     *
     * @param target 최종 결과값
     * @param srcs   이전 분기데이터
     * @param src2   마지막 분기데이터
     * @param key    분기데이터에서 찾을 값의 키
     * @param key2   최종결과로 저장할 키
     */
    private void setupCumulatedValue(Map<String, Object> target, List<Map<String, Object>> srcs, Map<String, Object> src2, String key, String key2) {
        String value = findValue(srcs, src2, key);
        if (value == null) {

        } else {
            target.put(key2, value);
        }

    }

    private String findValue(List<Map<String, Object>> srcs, Map<String, Object> src2, String key) {
        boolean isSameUnit = isSameUnit(srcs, src2, key);
        String value2 = Maps.getValue(src2, key);
        log.debug("마지막분기데이터 " + key + " " + value2);
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
            // 모든 값들을 원단위로 변환한후 src2의 단위로 변환하여 반환.
            String unitKey = findUnitKey(key);
            String unit2 = Maps.getValue(src2, unitKey);
            log.warn("unitKey={} unit={}", unitKey, unit2);
            // 단위값을 찾을 수 없어 처리할 수 없는 경우.
            if (isEmpty(unit2)) {
                return "";
            }
            String v2 = convertPrice(value2, unit2);

            long sum = findWonValue(srcs, key);
            if (sum == -1) {
                return "";
            }

            long v = toLong(v2) - sum;
            if (v == 0) {
                return "";
            } else {
                return convertMoney(v + "", unit2) + "";
            }
        }
    }

    private long findWonValue(List<Map<String, Object>> srcs, String key) {
        String unitKey = findUnitKey(key);
        long sum = 0;
        for (Map<String, Object> map : srcs) {
            String v = Maps.getValue(map, key);
            String u = Maps.getValue(map, unitKey);
            if (isEmpty(v)) {
                String period = Maps.getValue(map, "period");
                String isuCd = Maps.getValue(map, "isu_cd");
                log.error(isuCd + " " + period + " " + key + "에 해당하는 값이 없음. ");
                return -1;
            }

            String vv = convertPrice(v, u);
            sum += toLong(vv);
            log.debug(key + " sum=" + sum + " << " + v);
        }

        return sum;
    }

    private boolean isSameUnit(List<Map<String, Object>> srcs, Map<String, Object> src2, String key) {
        String unitKey = findUnitKey(key);
        String _unit = Maps.getValue(src2, unitKey);
        for (Map<String, Object> map : srcs) {
            String unit = Maps.getValue(map, unitKey);
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
            log.error(isuCd + " " + period + " " + key + "에 해당하는 값이 없음.");
            target.put(key, "");
            return;
        }

        if (isEmpty(value1)) {
            String period = Maps.getValue(src1, "period");
            String isuCd = Maps.getValue(src1, "isu_cd");
            log.error(isuCd + " " + period + " " + key + "에 해당하는 값이 없음.");
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

            long s = BizUtils.calculateWithDiffUnit(value1, unit1, value2, unit2, unit2);
            if (s == 0) {
                target.put(key, "");
            } else {
                target.put(key, BizUtils.toMoney(s));
            }
        }
    }

    private String findUnitKey(String key) {
        key = StringUtils.remove(key, "cum_");
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
                log.error(isuCd + " " + period + " " + key + "에 해당하는 값이 없음. ");
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

        return PERIODS.length + 1;
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
     *
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
        if (isEmpty(_map)) {
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
            return;
        }
        if (isOldAtCorrectedKongsi(_map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + _map);
            return;
        }

        log.info("실적관련 공시 처리중 " + key + " " + findAcptNoUrl(acptNo));
        String path = findPath(myContext.getHtmlTargetPath(), code, docNo, "htm");
        log.info("공시파일. " + path + " " + key);
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
        TableParser tableParser = new TableParser(selector);

        // 연결재무제표 분석..
        Map<String, Object> map = parseCfs(selector, tableParser, c, docUrl, myParser);
        // 개별재무제표 분석..
        Map<String, Object> map2 = parseFs(selector, tableParser, c, docUrl, myParser);

        if (isEmpty(map) && isEmpty(map2)) {
            log.error("연결재무제표, 재무제표 모두 없음. " + docUrl + " " + key);
            // 분석한 대상이 없더라도 최종 결과는 db에 처리하므로 여기에서 멈추지 않는다.
        }

        log.debug("연결재무재표 분석 결과 " + map);
        log.debug("개별재무재표 분석 결과 " + map2);

        if (isBanks(code)) {
            modifyBanksResult(map, map2, code, c, docUrl);
        } else {
            modifyNoBanksResult(map, map2, code, selector, myParser, tableParser, c, docUrl);
        }
        log.debug("연결재무재표 분석 결과2 " + map);
        log.debug("개별재무재표 분석 결과2 " + map2);
        setupCode(map, map2, code);
        Map<String, Object> params = findParams(map, map2, code, acptNo, docNm, key, findSummary(map));
        handleDb(params, map, map2, docNo, code, acptNo, docNm, key, updateCorrect);

    }

    /**
     * 이후 로직에서 code 가 필요함.
     *
     * @param map
     * @param map2
     * @param code
     */
    private void setupCode(Map<String, Object> map, Map<String, Object> map2, String code) {
        if (map != null) {
            map.put("CODE", code);
        }
        if (map2 != null) {
            map2.put("CODE", code);
        }
    }

    /**
     * 분석해야 할 재무제표 유형 확인
     * 보험사 중 삼성만 연결 나머지는 개별
     * 증권사는 연결만
     *
     * @param code
     * @return
     */
    private String findBankFinancialStatementType(String code) {
        if (isInsurance(code)) {
            if (isSamsungInsurance(code)) {
                return "CONNECT";
            } else {
                return "INDI";
            }
        } else if (isStock(code)) {
            return "CONNECT";
        } else {
            return "";
        }
    }

    /**
     * 금융실적 처리
     *
     * @param map    연결재무제표 분석데이터
     * @param map2   개별재무제표 분석데이터
     * @param code
     * @param c
     * @param docUrl
     * @throws Exception
     */
    private void modifyBanksResult(Map<String, Object> map, Map<String, Object> map2, String code, String c, String docUrl) throws Exception {
        if (isInsurance(code)) {
            //손해보험사
            modifyInsuranceResult(map, map2, code, c, docUrl);
        } else if (isStock(code)) {
            //증권사
            modifyStockResult(map, map2, code, c, docUrl);
        } else if (isLifeInsurance(code)) {
            //생명보험
            modifyLifeInsuranceResult(map, map2, code, c, docUrl);
        } else {

        }


    }

    /**
     * 생명보험 데이터 보정.
     *
     * @param map
     * @param map2
     * @param code
     * @param c
     * @param docUrl
     */
    private void modifyLifeInsuranceResult(Map<String, Object> map, Map<String, Object> map2, String code, String c, String docUrl) {
        if (map2 != null) {
            map2.put("REVENUE", "");
        }
        if (map != null) {
            map.put("REVENUE", "");
        }
    }

    /**
     * 증권사 실적 처리
     * 매출총이익이 순영업순익이며 영업이익과 판관비의 합임
     * 연결재무제표에서 분석가능함.
     * 한화투자증권 예외적임.
     *
     * @param map    연결재무제표
     * @param map2
     * @param code
     * @param c
     * @param docUrl
     */
    private void modifyStockResult(Map<String, Object> map, Map<String, Object> map2, String code, String c, String docUrl) {
        log.info("modifyStockResult");

        if (map2 != null) {
            map2.put("REVENUE", "");
        }
        if (map != null) {
            map.put("REVENUE", "");
        }
    }

    private boolean isHISC(String code) {
        return "A003530".equalsIgnoreCase(code);
    }

    private boolean isDaishin(String code) {
        return "A003540".equalsIgnoreCase(code);
    }

    /**
     * 손해보험사 처리.
     * 매출액은 경과보험료에서 추출함.
     * 나머지 영업이익과 순영업이익은 재무제표에서 추출
     * 삼성은 연결 나머지는 개별 재무제표..
     * 경과보험료는 모두 누적데이터여서 따로 테이블에 값을 관리해야 어렵지 않게 처리가능.
     * DB손해보험 예외적임..
     *
     * @param map    연결재무제표
     * @param map2   개별재무제표
     * @param code
     * @param c
     * @param docUrl
     */
    private void modifyInsuranceResult(Map<String, Object> map, Map<String, Object> map2, String code, String c, String docUrl) throws Exception {
        String[] keywords = new String[]{"영업", "현황"};
        String[] keywords2 = new String[]{"사업", "내용"};
        String[] noKeywords = new String[]{};

        FinancialStatementSelectorByPattern selector = new FinancialStatementSelectorByPattern();
        InsuranceMyParser myParser = new InsuranceMyParser();
        if (isSamsungInsurance(code)) {
            myParser.setTITLE_KEYWORDS(new String[]{"보험종목", "경과보험료"});
        }

        TableParser tableParser = new TableParser(selector);
        selector.setKeywords(keywords);
        selector.setNoKeywords(noKeywords);
        Map<String, Object> _map = (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);
        if (isEmpty(_map)) {
            // 영업의 현황 목차가 없는 공시가 있음..
            log.warn("영업의 현황 목차가없음.  " + docUrl);
            selector.setKeywords(keywords2);
            _map = (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);
        }
        String unit = Maps.findValue(_map, "UNIT");
        String take = "";
        String bankFinancialStatementType = findBankFinancialStatementType(code);
        if ("CONNECT".equalsIgnoreCase(bankFinancialStatementType)) {
            take = Maps.findValueWithKeys(_map, "경과보험료", "합계");
            setupTake(map, take, unit);
            if (isEmpty(map2) == false) {
                map2.clear();
            }
        } else if ("INDI".equalsIgnoreCase(bankFinancialStatementType)) {
            take = Maps.findValueWithKeys(_map, "경과보험료", "합계");
            if (":".equalsIgnoreCase(take)) {
                take = Maps.findValueWithKeys(_map, "손해", "합계");
            }
            setupTake(map2, take, unit);
            if (isEmpty(map) == false) {
                map.clear();
            }
        } else {

        }
    }

    private String modifyUnit(String unit) {
        unit = StringUtils.remove(unit, ",");
        unit = StringUtils.remove(unit, "%");
        return unit;
    }

    private String findUnit(String key) {
        String[] strings = StringUtils.substringsBetween(key, "(", ")");
        if (strings == null || strings.length == 0) {
            return "";
        } else {
            for (String s : strings) {
                if (s.endsWith("원")) {
                    return s;
                }
            }
            return "";
        }
    }

    /**
     * 최종 매출액 처리.
     *
     * @param map
     * @param take
     * @param _unit
     */
    private void setupTake(Map<String, Object> map, String take, String _unit) {
        String[] strings = StringUtils.split(take, ":");
        String takeKey = strings[0];
        String takeValue = strings[1];
        map.put("REVENUE", takeValue);
        map.put("REVENUE_KEY", takeKey);
        map.put("REVENUE_UNIT", _unit);

        if (takeKey.contains("반기")) {
            map.put("REVENUE_PERIOD", BANGI);
        } else if (takeKey.contains("분기")) {
            map.put("REVENUE_PERIOD", BUNGI);
        } else if (takeKey.contains("기")) {
            map.put("REVENUE_PERIOD", YUNGI);
        } else {
            map.put("REVENUE_PERIOD", "");
        }
//
//        _unit = modifyUnit(_unit);
//        if (StringUtils.isEmpty(_unit)) {
//            _unit = findUnit(takeKey);
//        }
//        String unit = Maps.findValue(map, "UNIT");
//        String _take = BizUtils.modifyWithUnit(takeValue, _unit);
//        String _revernue = BizUtils.convertMoney(_take + "", unit) + "";
//
//        String revenue = BizUtils.modifyWithComma(_revernue);
//        String revenueKey = takeKey;
//        map.put("REVENUE", revenue);
//        map.put("REVENUE_KEY", revenueKey);

    }

    private boolean isStock(String code) {
        return checkCode(STOCKS, code);
    }

    private boolean isLifeInsurance(String code) {
        return checkCode(LIFE_INSURANCES, code);
    }

    /**
     * NH투자증권 확인
     *
     * @param code
     * @return
     */
    private boolean isNHStock(String code) {
        return "A005940".equalsIgnoreCase(code);
    }

    /**
     * 삼성화재해상보험 확인
     *
     * @param code
     * @return
     */
    private boolean isSamsungInsurance(String code) {
        return "A000810".equalsIgnoreCase(code);
    }

    private boolean isSamsungStock(String code) {
        return "A016360".equalsIgnoreCase(code);
    }

    /**
     * 롯데손해보험 확인
     *
     * @param code
     * @return
     */
    private boolean isLotteInsurance(String code) {
        return "A000400".equalsIgnoreCase(code);
    }

    /**
     * 요약공시를 분석한 데이터인지 확인.
     *
     * @param map
     * @return
     */
    private boolean findSummary(Map<String, Object> map) {
        if (isEmpty(map)) {
            return false;
        }
        if (map.containsKey("IS_SUMMARY")) {
            return (Boolean) map.get("IS_SUMMARY");
        } else {
            return false;
        }
    }


    /**
     * 최종 데이터 DB 처리
     *
     * @param params        db에 저장할 데이터...
     * @param map           연결재무제표 분석 데이터
     * @param map2          개별재무젤표 분석 데이터
     * @param docNo
     * @param code
     * @param acptNo
     * @param docNm
     * @param key
     * @param updateCorrect
     */
    private void handleDb(Map<String, Object> params, Map<String, Object> map, Map<String, Object> map2, String docNo, String code, String acptNo, String docNm, String key, boolean updateCorrect) {

        params.put("doc_no", docNo);
        params.put("isu_cd", code);
        params.put("acpt_no", acptNo);
        params.put("acpt_dt", acptNo.substring(0, 8));

        correctEmptyValue(params, map, map2);
        setupPeriod(params, key);

        // 정정공시 처리
        if (docNm.contains("정정")) {
            List<String> _docNos = findBeforeKongsi(templateMapper, docNo, code, acptNo);
            for (String _docNo : _docNos) {
                log.info("정정공시중에 이전 공시 삭제... " + _docNo + " " + code);
                templateMapper.deleteBeforeBizPerfHolder(_docNo, code);
                deleteBeforeArticle(templateMapper, _docNo, code, acptNo, findArticleType());
            }
        }
        log.info("find SEQ " + params);
        Long seq = templateMapper.findBizPerfHolder(params);
        String period = Maps.getValue(params, "period");
        String subPk = code + "_" + period;
        if (seq == null) {
            templateMapper.insertBizPerfHolder(params);
            seq = Maps.getLongValue(params, "seq");
            log.info("INSERT 실적 " + params);
            if (isGoodArticle(docNm)) {
                sendToArticleQueue(rabbitTemplate, seq + "", findArticleType(), subPk);
            }
        } else {
            if (updateCorrect) {
                params.put("seq", seq);
                templateMapper.updateBizPerf(params);
                log.info("UPDATE 실적 " + params);
                sendToArticleQueue(rabbitTemplate, seq + "", findArticleType(), subPk, true);
            } else {
                log.info("이미 존재.. SKIP.. " + params);
                viewInfo(params);
            }
        }
        // 반기나 년기인 경우 분기데이터 생성처리를 시도해 본다.
        String periodType = Maps.getValue(params, "period_type");
        if (StringUtils.equalsAny(periodType, YUNGI, BANGI, QANGI)) {
            sendToCorrectQueue(rabbitTemplate, seq + "", periodType);
        }
    }

    private void viewInfo(Map<String, Object> param) {
        log.info("매출액={}", Maps.getValue(param, "revenue"));
        log.info("영업이익={}", Maps.getValue(param, "operating_profit"));
        log.info("당기순이익={}", Maps.getValue(param, "net_income"));
        log.info("연결매출액={}", Maps.getValue(param, "crevenue"));
        log.info("연결영업이익={}", Maps.getValue(param, "coperating_profit"));
        log.info("연결당기순이익={}", Maps.getValue(param, "cnet_income"));
    }

    /**
     * @param map         연결재무제표 분석데이터
     * @param map2        개별재무제표 분석데이터
     * @param code
     * @param selector
     * @param myParser
     * @param tableParser
     * @param c           공시html
     * @param docUrl
     * @throws Exception
     */
    private void modifyNoBanksResult(Map<String, Object> map, Map<String, Object> map2, String code, FinancialStatementSelectorByPattern selector, PerformanceMyParser myParser, TableParser tableParser, String c, String docUrl) throws Exception {
        String rawPeriod = "";
        if (isEmpty(map) == false) {
            rawPeriod = Maps.getValue(map, "PERIOD");
        }
        String indiRawPeriod = "";
        if (isEmpty(map2) == false) {
            indiRawPeriod = Maps.getValue(map2, "PERIOD");
        }
// 요약 연결,개별 확인할 수 있게 하려고..
        boolean existCfsMap = false;
        boolean existifsMap = false;

        boolean isSummary = false;

        // 단위가 원화가 아닌 경우 요약재무정보에서 가져오게 처리해야 함.
        String unit = "";
        if (isEmpty(map) == false) {
            unit = Maps.getValue(map, "UNIT");
            existCfsMap = true;
        }
        String unit2 = "";
        if (isEmpty(map2) == false) {
            unit2 = Maps.getValue(map2, "UNIT");
            existifsMap = true;
        }


        if (BizUtils.isNotWon(unit) || BizUtils.isNotWon(unit2)) {
            if (isBanks(code) == false) {
                // 연결이나 개별 중에 하나라도 다른 통화를 사용하면 연결요약과 개별요약 모두 분석한다.
                isSummary = true;
                selector.setKeywords(new String[]{"요약", "재무"});
                selector.setNoKeywords(new String[]{});
                if (existCfsMap) {
                    log.debug("... 요약연결재무제표");
                    myParser.setSummrayCase(true);
                    myParser.setIndividual(false);
                    Map<String, Object> _map = (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);
                    // 요약 연결 period가 없는 경우 보정.
                    if (isEmpty(_map) == false) {
                        _map.put("RAW_PERIOD", rawPeriod);
                        // 기존 데이터를 날리고 요약연결재무제표 데이터로 대체한다.
                        map.clear();
                        map.putAll(_map);
                        map.put("IS_SUMMARY", true);
                    }
                }
                if (existifsMap) {
                    log.debug("... 요약개별재무제표");
                    myParser.setSummrayCase(true);
                    myParser.setIndividual(true);
                    Map<String, Object> _map2 = (Map<String, Object>) tableParser.simpleParse(c, docUrl, myParser);
                    if (isEmpty(_map2) == false) {
                        // 요약 개별 period가 없는 경우 보정.
                        _map2.put("RAW_PERIOD", indiRawPeriod);

                        map2.clear();
                        map2.putAll(_map2);
                        map2.put("IS_SUMMARY", true);
                        if (existCfsMap && existifsMap) {
                            String tableKey = Maps.getValue(map, "TABLE_KEY");
                            String tableKey2 = Maps.getValue(_map2, "TABLE_KEY");
                            if (tableKey.equalsIgnoreCase(tableKey2)) {
                                log.error("요약 개별과 연결재무정보가 동일함.. ");
                                map2.clear();
                                map2 = null;
                            }
                        }

                    }
                }

                log.debug("요약연결재무재표 분석 결과 " + map);
                log.debug("요약개별재무재표 분석 결과 " + map2);
            }

        }
    }

    /**
     * 잘못된 공시로 인해
     * 매출액이 누락되었을 때 현재 누적매출에서 이전분기 누적매출을 빼어서 데이터를 보정처리함.
     * 매출액이나 영업어익 순이익 모두 대상이 될 수 있음.
     *
     * @param params
     * @param map
     * @param map2   개별
     */
    private void correctEmptyValue(Map<String, Object> params, Map<String, Object> map, Map<String, Object> map2) {
        String period = Maps.getValue(params, "period");
        if (StringUtils.containsAny(period, "1Q", "2Q", "3Q", "4Q") == false) {
            // 누적분기가 아닌 공시를 대상으로 처리하므로
            return;
        }

        String code = Maps.getValue(params, "isu_cd");
        String previousPeriod = findPeriviousPeriod(period);
        if (isEmpty(previousPeriod)) {
            return;
        }
        Map<String, Object> previous = templateMapper.findPreviousBizPerf(code, previousPeriod);
        if (previous == null) {
            return;
        }
        if (isEmpty(map) == false) {
            correctConnectEmptyValue(params, previous);
        }
        if (isEmpty(map2) == false) {
            correctIndividualEmptyValue(params, previous);
        }
    }

    /**
     * 개별재무재표 값 보정.
     *
     * @param params
     */
    private void correctIndividualEmptyValue(Map<String, Object> params, Map<String, Object> previous) {
        String revenue = Maps.getValue(params, "revenue");
        String operatingProfit = Maps.getValue(params, "operating_profit");
        String netIncome = Maps.getValue(params, "net_income");

        if (StringUtils.isAnyEmpty(revenue, operatingProfit, netIncome) == false) {
            return;
        }

        if (isEmpty(revenue)) {
            correctEmptyValue(params, previous, "cum_revenue", "revenue", "unit");
        }
        if (isEmpty(operatingProfit)) {
            correctEmptyValue(params, previous, "cum_operating_profit", "operating_profit", "unit");
        }

        if (isEmpty(netIncome)) {
            correctEmptyValue(params, previous, "cum_net_income", "net_income", "unit");
        }

    }

    /**
     * @param params   현분기 정보
     * @param previous 이전분기 정보
     * @param key      누적빼기 대상
     * @param key2     누적빼기 결과 설정대상.
     * @param unitKey  단위키 연결과 개별 단위가 달라서 단위키가 필요
     */
    private void correctEmptyValue(Map<String, Object> params, Map<String, Object> previous, String key, String key2, String unitKey) {
        log.debug("..{}..{},{}", key, key2, unitKey);
        log.debug("NOW {}", params);
        log.debug("previous {}", previous);
        String nowUnit = Maps.getValue(params, unitKey);
        nowUnit = nowUnit.trim();

        if (isEmpty(nowUnit)) {
            return;
        }
        String previousUnit = Maps.getValue(previous, unitKey);
        previousUnit = previousUnit.trim();

        String value2 = Maps.getValue(params, key);
        String value1 = Maps.getValue(previous, key);

        if (StringUtils.equalsIgnoreCase(nowUnit, previousUnit)) {
            long l2 = findMoney(value2);
            long l1 = findMoney(value1);
            long v = l2 - l1;
            if (v == 0) {
                params.put(key2, "");
            } else {
                params.put(key2, BizUtils.toMoney(v));
            }
        } else {
            long s = BizUtils.calculateWithDiffUnit(value1, previousUnit, value2, nowUnit, nowUnit);
            if (s == 0) {
                params.put(key, "");
            } else {
                params.put(key, BizUtils.toMoney(s));
            }
        }
    }


    private String findPeriviousPeriod(String period) {
        String q = period.substring(5, 6);

        String y = period.substring(0, 4);

        if ("1".equalsIgnoreCase(q)) {
            int _y = Integer.valueOf(y) - 1;
            return _y + "/4Q";
        } else {
            return y + "/" + (Integer.valueOf(q) - 1) + "Q";
        }
    }

    /**
     * 연결재무제표 값 보정.
     *
     * @param params
     */
    private void correctConnectEmptyValue(Map<String, Object> params, Map<String, Object> previous) {
        String crevenue = Maps.getValue(params, "crevenue");
        String coperatingProfit = Maps.getValue(params, "coperating_profit");
        String cnetIncome = Maps.getValue(params, "cnet_income");

        // 연결재무제표가 없는 경우는 SKIP
        if (StringUtils.isAllEmpty(crevenue, coperatingProfit, cnetIncome)) {
            return;
        }

        if (isEmpty(crevenue)) {
            correctEmptyValue(params, previous, "cum_crevenue", "crevenue", "unit2");
        }
        if (isEmpty(coperatingProfit)) {
            correctEmptyValue(params, previous, "cum_coperating_profit", "coperating_profit", "unit2");
        }

        if (isEmpty(cnetIncome)) {
            correctEmptyValue(params, previous, "cum_cnet_income", "cnet_income", "unit2");
        }


    }

    private Map<String, Object> findEstateMap(String c, String docUrl, TableParser tableParser, EstateMyParser estateMyParser) throws Exception {
        return (Map<String, Object>) tableParser.simpleParse(c, docUrl, estateMyParser);
    }

    /**
     * FIXME
     *
     * @param code
     * @return
     */
    private boolean isBanks(String code) {

//        return false;
//        if (checkCode(BANKS, code)) {
//            return checkCode(INSURANCES, code);
//        } else {
//            return false;
//
//        }
        return checkCode(BANKS, code);
    }

    /**
     * 보험 확인
     *
     * @param code
     * @return
     */
    private boolean isInsurance(String code) {
        return checkCode(INSURANCES, code);
    }

    private boolean checkCode(String[] codes, String code) {
        for (String s : codes) {
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
                    params.put("period_type", QANGI);
                } else if (period.contains("Q")) {
                    params.put("period_type", BUNGI);
                } else if (period.contains("Y")) {
                    params.put("period_type", YUNGI);
                } else if (period.contains("SA")) {
                    params.put("period_type", BANGI);
                }
            }
        }
    }

    private boolean isEmpty(Map<String, Object> map) {
        if (map == null || map.size() == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 금융 매출액 정보가 있는지 확인
     *
     * @param map
     * @return
     */
    private String findRevenue(Map<String, Object> map) {
        return Maps.getValue(map, "REVENUE");
    }


    /**
     * 재무제표 분석 결과 토대로 최종 DB 처리 데이터 추출
     *
     * @param map       연결재무제표 분석데이터
     * @param map2      개별재무제표 분석데이터
     * @param acptNo
     * @param docNm
     * @param key
     * @param isSummary 요약에서 분석하였는지 여부
     * @return
     */
    private Map<String, Object> findParams(Map<String, Object> map, Map<String, Object> map2, String code, String acptNo, String docNm, String key, boolean isSummary) {
        Map<String, Object> params = new HashMap<>();
        // 두개의 재무제표를 모두 분석하지 못함..
        if (isEmpty(map) && isEmpty(map2)) {
            return params;
        }

        boolean hasConsolidatedStock = false;
        if (isEmpty(map) == false) {

            Performance performance = BizUtils.findPerformance(map, isSummary);
            log.info("연결재무제표실적 " + performance);
            performance.setFromData("연결재무제표");
            performance.setDocNm(docNm);
            performance.setKey(key);


            String revenuePeriod = Maps.getValue(map, "REVENUE_PERIOD");
            // 연결재무제표인 경우에..
            performance.setUnit2(performance.getUnit());
            performance.setAcptUrl(KrxCrawlerHelper.getItemUrl(acptNo));
            String period = BizUtils.findPeriod(performance.getPeriod(), docNm);
            if (hasThreeMonthInKey(performance)) {
                if (StringUtils.containsAny(revenuePeriod, BANGI, YUNGI)) {
                    performance.setParsedPeriod(period);
                    log.info("PERIOD분석 " + performance.getPeriod() + " " + docNm + " << " + period);

                    ////////////////// 금융매출액은 반기데이터인데 나머지 영업이익이나 순이익은 분기데이터인 경우 데이터를 반기데이터로 처리하기 위한 로직
                    setupCNetIncome(performance, map, revenuePeriod);
                    setupCOperrationProfit(performance, map, revenuePeriod);

                } else {
                    String _period = BizUtils.findPeriod(performance.getPeriod(), docNm, true);
                    performance.setParsedPeriod(_period);
                    String _docNm = findDocNm(docNm);
                    log.info("3개월 PERIOD분석 " + performance.getPeriod() + " " + _docNm + " << " + _period);
                }


            } else {
                performance.setParsedPeriod(period);
                log.info("PERIOD분석 " + performance.getPeriod() + " " + docNm + " <<  " + period);
            }
            if (map.containsKey("REVENUE")) {
                if (isInsurance(code)) {
                    // 손해보험사인 경우 이미 분석된 값으로 매출액을 처리함.
                    String revenue = Maps.getValue(map, "REVENUE");
                    if (isEmpty(revenue) == false) {
                        String revenueKey = Maps.getValue(map, "REVENUE_KEY");
                        String unit = Maps.getValue(map, "REVENUE_UNIT");
                        performance.setupTake(revenueKey, revenue, unit);
                    }
                } else if (isStock(code)) {
                    // 증권사는 순영업손익 = 영업이익 + 판매관리비 로 처리한다.
                    // 연결에서 찾는다. 하지만 작은 회사인 경우 개별만 있을 수 있다.
                    if (isSamsungStock(code)) {
                        //순영업손익이 있다.

                        modifyStockPerfAsNetOperationIncome(performance, map);
                    } else {
                        //키움, NH투자증권OK
                        // 대체로 영업이익 + 판매관리비
                        modifyStockPerfAsPlu(performance, map);
                    }
                    hasConsolidatedStock = true;
                } else if (isLifeInsurance(code)) {
                    // 매출액 = 보험영업수익-보험영업비용
                    // 영업이익 = 영업이익 + 재산관리비
                    // 당기순이익은 그대로
                    modifyLifeInsurance(performance, map);

                }
            } else {
                log.info("REVENUE 가 없음. " + map);
            }

            performance.param(params);

            params.put("unit", Maps.getValue(map, "UNIT"));
        }


        if (isEmpty(map2) == false) {
            Performance performance = BizUtils.findPerformance(map2, isSummary);
            log.info("재무제표실적 " + performance);
            performance.setFromData("재무제표");
            performance.setDocNm(docNm);
            performance.setKey(key);
            performance.setAcptUrl(KrxCrawlerHelper.getItemUrl(acptNo));
            String revenuePeriod = Maps.getValue(map2, "REVENUE_PERIOD");
            if (hasThreeMonthInKey(performance)) {
                if (StringUtils.containsAny(revenuePeriod, BANGI, YUNGI)) {
                    String period = BizUtils.findPeriod(performance.getPeriod(), docNm);
                    performance.setParsedPeriod(period);
                    log.info("PERIOD분석 " + performance.getPeriod() + " " + docNm + " << " + period);
                } else {
                    String period = BizUtils.findPeriod(performance.getPeriod(), docNm, true);
                    performance.setParsedPeriod(period);
                    String _docNm = findDocNm(docNm);
                    log.info("3개월 PERIOD분석 " + performance.getPeriod() + " " + _docNm + " << " + period);
                }

            } else {
//                log.info("PERIOD분석 " + performance.getPeriod() + " " + docNm);
//                performance.setParsedPeriod(BizUtils.findPeriod(performance.getPeriod(), docNm));
                String period = BizUtils.findPeriod(performance.getPeriod(), docNm);
                performance.setParsedPeriod(period);
                log.info("PERIOD분석 " + performance.getPeriod() + " " + docNm + " << " + period);
            }
            // 금융실적인지 여부
            if (map2.containsKey("REVENUE")) {
                if (isInsurance(code)) {
                    //TODO 추후 변경..
                    String revenue = Maps.getValue(map2, "REVENUE");
                    if (isEmpty(revenue) == false) {
                        String revenueKey = Maps.getValue(map2, "REVENUE_KEY");
                        String unit = Maps.getValue(map2, "REVENUE_UNIT");
                        performance.setupTake(revenueKey, revenue, unit);
                    }
                } else if (isStock(code)) {
                    //유화증권, 한화투자증권
                    if (hasConsolidatedStock == false || isHISC(code)) {
                        log.info("MAP2={}", map2);
                        modifyStockPerfAsPlu(performance, map2);
                    }

                } else if (isLifeInsurance(code)) {
                    modifyLifeInsurance(performance, map2);
                }
            } else {
                log.info("REVENUE 가 없음. " + map2);
            }
            performance.param(params);
            params.put("unit2", Maps.getValue(map2, "UNIT"));
        }

        // 한화투자증권 데이터 보정처리.
        if (isHISC(code)) {
            //개별 매출액을 연결매출액으로 업데이트 처리
            params.put("crevenue", params.get("revenue"));
            params.put("cum_crevenue", params.get("cum_revenue"));
        }


        return params;
    }

    private void modifyLifeInsurance(Performance performance, Map<String, Object> map) {

        // 매출액 = 보험영업수익-보험영업비용
        modifyLifeInsuranceRevenue(performance, map);
        // 영업이익 = 영업이익 + 재산관리비
        modifyLifeInsuranceOperationProfit(performance, map);

        // 당기순이익은 그대로
    }

    private void modifyLifeInsuranceOperationProfit(Performance performance, Map<String, Object> map) {
        log.debug("생명보험 영업이익 처리중..  ");
        //영업이익+재산관리비
        // 3개월 처리.
        if (StringUtils.isAnyEmpty(performance.getOperatingProfit(), performance.getOperatingProfitKey()) == false) {
            modifyLifeInsuranceOperationProfit(performance, map,performance.getOperatingProfit(),performance.getOperatingProfitKey(), false);
        }
        if (hasThreeMonthInKey(performance)) {
            // 누적 처리
            if (StringUtils.isAnyEmpty(performance.getCumulatedOperatingProfit(), performance.getCumulatedOperatingProfitKey()) == false) {
                modifyLifeInsuranceOperationProfit(performance, map, performance.getCumulatedOperatingProfit(),performance.getCumulatedOperatingProfitKey(),true);
            }
        }
    }

    private void modifyLifeInsuranceOperationProfit(Performance performance, Map<String, Object> map,String operationProfit,String operationProfitKey, boolean isCumulated) {
        String previousKey = findPreviousKey(operationProfitKey);

        String[] values1 = findValueWithKeys(map, isCumulated, previousKey, "재산관리비");
        if (values1.length != 2) {
            log.warn("재산관리비 찾을 수 없음. {} ", previousKey);
            return;
        }

        String value = plusMoneyString(operationProfit, values1[1]);
        log.info("조정영업이익({}) = 영업이익({}) + 재산관리비({}) ", value, operationProfit, values1[1]);

        if (isCumulated) {
            performance.setupCumulatedProfit("__"+previousKey, value);
        } else {
            performance.setupProfit("__"+previousKey, value);
        }
    }

    /**
     * 매출액 보험영업수익-보험영업비용
     *
     * @param performance
     * @param map
     */
    private void modifyLifeInsuranceRevenue(Performance performance, Map<String, Object> map) {
        log.debug("생명보험 매출액 처리중..  ");
        //보험영업수익-보험영업비용
        // 3개월 처리.
        modifyLifeInsuranceRevenue(performance, map, false);
        if (hasThreeMonthInKey(performance)) {
            // 누적 처리
            modifyLifeInsuranceRevenue(performance, map, true);
        }

    }

    private String findPreviousKey(Performance performance) {
        if (isEmpty(performance.getRevenue())==false) {
            return findPreviousKey(performance.getRevenueKey());
        } else if (isEmpty(performance.getOperatingProfit()) == false) {
            return findPreviousKey(performance.getOperatingProfitKey());
        } else if (isEmpty(performance.getNetIncome()) == false) {
            return findPreviousKey(performance.getNetIncomeKey());
        } else {
            return "";

        }
    }

    private void modifyLifeInsuranceRevenue(Performance performance, Map<String, Object> map, boolean isCumulated) {
        //보험영업수익-보험영업비용
        String previousKey = findPreviousKey(performance);
        String[] values1 = findValueWithKeys(map, isCumulated, previousKey, "보험료","수익");
        if (values1.length != 2) {
            log.warn("보험료수익을 찾을 수 없음. {} ", previousKey);
            return;
        }
        String[] values2 = findValueWithKeys(map, isCumulated, previousKey, "재보험","수익");
        if (values2.length != 2) {
            log.warn("재보험수익을 찾을 수 없음. {} ", previousKey);
            return;
        }
        String buzIncome = plusMoneyString(values1[1], values2[1]);
        log.info("보험영업수익({}) = 보험료수익({}) + 재보험수익({}) ", buzIncome, values1[1], values2[1]);


        String[] values3 = findValueWithKeys(map, isCumulated, false,previousKey, "지급보험금");
        String[] values4 = findValueWithKeys(map, isCumulated, false,previousKey, "비용보험금");
        String[] values5 = findValueWithKeys(map, isCumulated, false,previousKey, "보험금","기타","지급비용");
        String[] values6 = findValueWithKeys(map, isCumulated, false,previousKey, "재보험비용");
        String[] values7 = findValueWithKeys(map, isCumulated, false,previousKey, "사업비");
        String[] values8 = findValueWithKeys(map, isCumulated, false,previousKey, "신계약상각비");
        String[] values9 = findValueWithKeys(map, isCumulated, false,previousKey, "신계약","비상각비");

        String buzExpense = plusMoneyString(values3[1], values4[1],values5[1],values6[1],values7[1],values8[1],values9[1]);
        log.info("보험영업비용({}) = 지급보험금({}) + 보험금비용({})+ 보험금및기타보험제지급비용({})+ 재보험비용({})+ 사업비({})+ 신계약상각비({})+ 신계약비상각비({})", buzExpense, values3[1], values4[1],values5[1],values6[1],values7[1],values8[1],values9[1]);

        String crevenue = minusMoneyString(buzIncome, buzExpense);
        if (isCumulated) {
            performance.setupCumulatedTake("__생명보험키", crevenue);
        } else {
            performance.setupTake("__생명보험키", crevenue);
        }
    }


    /**
     * 순영업순익이 있는 경우에는 바로 처리함.
     *
     * @param performance
     * @param map
     */
    private void modifyStockPerfAsNetOperationIncome(Performance performance, Map<String, Object> map) {

        modifyStockPerfAsNetOperationIncome(performance, map, performance.getOperatingProfitKey(), false);

        if (hasThreeMonthInKey(performance)) {

            modifyStockPerfAsNetOperationIncome(performance, map, performance.getCumulatedOperatingProfitKey(), true);
        }
    }


    private void modifyStockPerfAsNetOperationIncome(Performance performance, Map<String, Object> map, String operationProfitKey, boolean isCumulated) {
        String cumulatedPreviousKey = findPreviousKey(operationProfitKey);
        String cumulatedRevenueKeyValue = Maps.findValueWithKeys(map, cumulatedPreviousKey, "순영업", "손익");
        String[] strings = StringUtils.split(cumulatedRevenueKeyValue, ":");
        String revenueKey = strings[0];
        String revenueValue = strings[1];
        if (isCumulated) {
            performance.setupCumulatedTake(revenueKey, revenueValue);
        } else {
            performance.setupTake(revenueKey, revenueValue);
        }
    }

    /**
     * 판매비와 관리비를 영업이익에 더해 매출액 처리...
     * 3개월과 누적 모두 처리.
     *
     * @param performance
     * @param map
     */
    private void modifyStockPerfAsPlu(Performance performance, Map<String, Object> map) {

        // 3개월 처리.
        if (StringUtils.isAnyEmpty(performance.getOperatingProfit(), performance.getOperatingProfitKey()) == false) {
            modifyStockPerfAsPlu(performance, map, performance.getOperatingProfit(), performance.getOperatingProfitKey(), false);
        }
        if (hasThreeMonthInKey(performance)) {
            // 누적 처리
            if (StringUtils.isAnyEmpty(performance.getCumulatedOperatingProfit(), performance.getCumulatedOperatingProfitKey()) == false) {
                modifyStockPerfAsPlu(performance, map, performance.getCumulatedOperatingProfit(), performance.getCumulatedOperatingProfitKey(), true);
            }
        }


    }

    /**
     * 매출액 재처리.
     *
     * @param performance
     * @param map
     * @param operationProfit
     * @param operationProfitKey
     * @param isCumulated        누적여부
     */
    private void modifyStockPerfAsPlu(Performance performance, Map<String, Object> map, String operationProfit, String operationProfitKey, boolean isCumulated) {
        log.debug(".. operationProfit={} operationProfitKey={} isCumulated={} ", operationProfit, operationProfitKey, isCumulated);
        String previousKey = findPreviousKey(operationProfitKey);
        for (String key : map.keySet()) {
//            log.debug("{}={}", key, map.get(key));
        }

        String code = Maps.getValue(map, "CODE");
        if (isHISC(code)) {
            //한화투자증궈
            log.debug("한화투자증권 순영업이익 처리중..  ");
            //순영업수익 = 영업손익 + 인건빈 + 기타일반관리비
            String[] laborCostsKeyValue = findValueWithKeys(map, isCumulated, previousKey, "인건비");
            if (laborCostsKeyValue.length != 2) {
                log.warn("인건비를 찾을 수 없음. {} ", previousKey);
                return;
            }
            String[] otherGeneralKeyValue = findValueWithKeys(map, isCumulated, previousKey, "기타", "일반", "관리비");
            if (otherGeneralKeyValue.length != 2) {
                log.warn("기타일반관리비 찾을 수 없음. {} ", previousKey);
                return;
            }
            String crevenue = plusMoneyString(operationProfit, laborCostsKeyValue[1], otherGeneralKeyValue[1]);
            log.info("총매출액({}) = 영업손익({}) + 인건비({}) +기타일반관리비({}) ", crevenue, operationProfit, laborCostsKeyValue[1], otherGeneralKeyValue[1]);
            if (isCumulated) {
                performance.setupCumulatedTake(operationProfitKey + laborCostsKeyValue[0] + otherGeneralKeyValue[0], crevenue);
            } else {
                performance.setupTake(operationProfitKey + laborCostsKeyValue[0] + otherGeneralKeyValue[0], crevenue);
            }

        } else if (isDaishin(code)) {
            //대신증권 영업손익+인건비+감가상각비 및 상각비+기타 판매비와관리비
            log.debug("대신증권 순영업이익 처리중..  ");
            String[] laborCostsKeyValue = findValueWithKeys(map, isCumulated, previousKey, "인건비");
            if (laborCostsKeyValue.length != 2) {
                log.warn("인건비를 찾을 수 없음. {} ", previousKey);
                return;
            }
            String[] depreExpKeyValue = findValueWithKeys(map, isCumulated, previousKey, "감가상각비", "상각비");
            if (depreExpKeyValue.length != 2) {
                log.warn("감가상각비 및 상각비 찾을 수 없음. {} ", previousKey);
                return;
            }
            String[] otherGeneralKeyValue = findValueWithKeys(map, isCumulated, previousKey, "판매비", "관리비", "기타");
            if (otherGeneralKeyValue.length != 2) {
                log.warn("기타 판매비와관리비 찾을 수 없음. {} ", previousKey);
                return;
            }
            String crevenue = plusMoneyString(operationProfit, laborCostsKeyValue[1], depreExpKeyValue[1], otherGeneralKeyValue[1]);
            log.info("총매출액({}) = 영업손익({}) + 인건비({}) +감가상각비 및 상각비({})+ 기타 판매비와관리비({})", crevenue, operationProfit, laborCostsKeyValue[1], depreExpKeyValue[1], otherGeneralKeyValue[1]);
            if (isCumulated) {
                performance.setupCumulatedTake(operationProfitKey + laborCostsKeyValue[0] + depreExpKeyValue[0] + otherGeneralKeyValue[0], crevenue);
            } else {
                performance.setupTake(operationProfitKey + laborCostsKeyValue[0] + depreExpKeyValue[0] + otherGeneralKeyValue[0], crevenue);
            }


        } else {
            log.debug("일반 투자증권 순영업이익 처리중..  ");
            // 대체로 영업이익 + 판매관리비
            String salesManagementKeyValue = Maps.findValueWithKeys(map, previousKey, "판매", "관리비");
            log.debug("previousKey={},salesManagementKeyValue={} isCumulated={}", previousKey, salesManagementKeyValue, isCumulated);
            String[] strings = StringUtils.split(salesManagementKeyValue, ":");
            if (strings.length == 2) {
                String salesManagementKey = strings[0];
                String salesManagementValue = strings[1];
                log.info("총매출액({}) = 영업이익({}) + 판매관리비({}) ", "", operationProfit, salesManagementValue);
                String crevenue = plusMoneyString(operationProfit, salesManagementValue);
                log.info("총매출액({}) = 영업이익({}) + 판매관리비({}) ", crevenue, operationProfit, salesManagementValue);
                if (isCumulated) {
                    performance.setupCumulatedTake(operationProfitKey + salesManagementKey, crevenue);
                } else {
                    performance.setupTake(operationProfitKey + salesManagementKey, crevenue);
                }
            } else {
                log.warn("판매관리비를 찾을 수 없음. {}판매_관리비", previousKey);
            }
        }

    }

    private String[] findValueWithKeys(Map<String, Object> map, boolean isCumulated, String... keys) {
        return findValueWithKeys(map,isCumulated,true,keys);
    }
    private String[] findValueWithKeys(Map<String, Object> map, boolean isCumulated, boolean isStrictly, String... keys) {
        String keyValue = Maps.findValueWithKeys(map, keys);
        log.debug("previousKey={},salesManagementKeyValue={} isCumulated={}", keys[0], keyValue, isCumulated);
        String[] strings = StringUtils.split(keyValue, ":");
        if (isStrictly) {
            return strings;
        } else {
            if (strings.length == 2) {
                return strings;
            } else {
                String[] results = new String[2];
                results[0] = "";
                results[1] = "0";
                return results;
            }
        }
    }

    private boolean isYuwha(String code) {
        return "A003460".equalsIgnoreCase(code);
    }

    private String plusMoneyString(String a, String b) {
        String _a = BizUtils.findNumericString(a);
        long aa = 0;
        if (StringUtils.substringBetween(a, "(", ")") == null) {
            aa = Long.valueOf(_a);
        } else {
            aa = Long.valueOf(_a) * (-1);
        }

        String _b = BizUtils.findNumericString(b);
        long bb = Long.valueOf(_b);
        return BizUtils.toMoney(aa + bb);
    }
    private String minusMoneyString(String a, String b) {
        String _a = BizUtils.findNumericString(a);
        long aa = 0;
        if (StringUtils.substringBetween(a, "(", ")") == null) {
            aa = Long.valueOf(_a);
        } else {
            aa = Long.valueOf(_a) * (-1);
        }

        String _b = BizUtils.findNumericString(b);
        long bb = Long.valueOf(_b);
        return BizUtils.toMoney(aa - bb);
    }
    private String plusMoneyString(String... values) {
        long total = 0;
        for (String value : values) {

            total += toMoneyLong(value);
        }

        return BizUtils.toMoney(total);
    }

    private long toMoneyLong(String a) {
        String _a = BizUtils.findNumericString(a);
        long aa = 0;
        if (StringUtils.substringBetween(a, "(", ")") == null) {
            aa = Long.valueOf(_a);
        } else {
            aa = Long.valueOf(_a) * (-1);
        }
        return aa;
    }

    private String findPreviousKey(String key) {
        int index = key.lastIndexOf("_");
        return key.substring(0, index);
    }

    /**
     * 영업이익 보정.
     *
     * @param performance
     * @param map
     * @param revenuePeriod
     */
    private void setupCOperrationProfit(Performance performance, Map<String, Object> map, String revenuePeriod) {
//        String key = performance.getCumulatedOperatingProfitKey();
//        if (revenuePeriod.equalsIgnoreCase(BANGI)) {
//            key = StringUtils.replace(key, "3개월", "누적");
//        } else if (revenuePeriod.equalsIgnoreCase(YUNGI)) {
//            key = StringUtils.replace(key, "3개월", "누적");
//        }
//        if (map.containsKey(key)) {
//            String value = Maps.getValue(map, key);
//            log.info("순영업이익. 키={} 값={} 누적키={},누적값={}", key, value,performance.getCumulatedOperatingProfitKey(),performance.getCumulatedOperatingProfit());
//            performance.setOperatingProfitKey(key);
//            performance.setCumulatedOperatingProfit(value);
//        }

        log.info("순영업이익. 누적키={},누적값={}", performance.getCumulatedOperatingProfitKey(), performance.getCumulatedOperatingProfit());
        performance.setOperatingProfitKey(performance.getCumulatedOperatingProfitKey());
        performance.setOperatingProfit(performance.getCumulatedOperatingProfit());
    }

    /**
     * 순이익 정보 보정처리.
     * 3개월연결순이익을 반기연결순이익으로 변경함.
     *
     * @param performance
     * @param map
     * @param revenuePeriod
     */
    private void setupCNetIncome(Performance performance, Map<String, Object> map, String revenuePeriod) {
//        String key = performance.getCumulatedNetIncomeKey();
//        if (revenuePeriod.equalsIgnoreCase(BANGI)) {
//            key = StringUtils.replace(key, "3개월", "누적");
//        } else if (revenuePeriod.equalsIgnoreCase(YUNGI)) {
//            key = StringUtils.replace(key, "3개월", "누적");
//        }
//        if (map.containsKey(key)) {
//            String value = Maps.getValue(map, key);
//            log.info("순이익보정. 키={} 값={} 누적키={},누적값={}", key, value,performance.getCumulatedNetIncomeKey(),performance.getCumulatedNetIncome());
//            performance.setCumulatedNetIncomeKey(key);
//            performance.setCumulatedNetIncome(value);
//        }
        log.info("순이익보정. 누적키={},누적값={}", performance.getCumulatedNetIncomeKey(), performance.getCumulatedNetIncome());
        performance.setNetIncomeKey(performance.getCumulatedNetIncomeKey());
        performance.setNetIncome(performance.getCumulatedNetIncome());
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

        if (containsAny(revenueKey, "누적", "반기", "분기")) {
            return false;
        }
        if (containsAny(operatingProfitKey, "누적", "반기", "분기")) {
            return false;
        }
        if (containsAny(netIncomeKey, "누적", "반기", "분기")) {
            return false;
        }

        //20220214002507_A001720_20220214000887
        return true;

    }

    private boolean containsAny(String src, String... ss) {
        return StringUtils.containsAny(src, ss);
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
