package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.model.Rumor;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.MyTakingParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.TakingSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.Tables;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.model.Taking;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * 취득 템플릿 처리
 * 일단 주식등의 대량보유상황보고서 공시에서 추출하여 분석함.
 */
@Slf4j
@Component
public class TakingHolderCommand extends BasicCommand {
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

        if (acptNm.contains("정정")) {
            if (docNm.contains("정정") == false) {
                log.info("정정공시중에 이전공시임.. " + key);
                return;
            }
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

        String html = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, loadBalancerCommandHelper);
        if (isEmpty(html)) {
            log.error("공시Html을 수집할 수 없음. " + key + " " + docUrl);
            return;
        }

        TableParser parser = new TableParser(new TakingSelectorByPattern());
        List<Table> tables = parser.parseSome(html, docUrl, new MyTakingParser());
        List<Map<String, Object>> takings = new ArrayList<>();
        for (Table t : tables) {
            if ("차임금".equalsIgnoreCase(t.getTitle())) {
                takings.add(Tables.widTableToMap(t));
            } else {
                List<Map<String, Object>> maps = t.toMapList();
                for (Map<String, Object> _map : maps) {
                    takings.add(_map);
                }
            }
        }

        if (takings.size() == 0) {
            log.info("취득데이터가 없음.  " + key + " " + docUrl);
            return;
        }

        for (Map<String, Object> _map : takings) {
            log.debug("<<< "+_map);
        }
        Map<String, Taking> takingMap = aggregate(takings);
        handleBeforeTaking(code, acptNo);
        for (String name : takingMap.keySet()) {
            Taking taking = takingMap.get(name);
            log.info(taking.toString());
        }

    }

    private void handleBeforeTaking(String code, String acptNo) {
        //        for (Kongsi.UrlInfo urlInfo : urlInfos) {
//            String docNm = urlInfo.getDocNm();
//            if (docNm.contains("정정") == false) {
//                String docNo = urlInfo.getDocNo();
//                log.info("정정이전공시 삭제 " + docNo + "_" + code + "_" + acptNo);
//                templateMapper.deleteOverlapRumor(docNo, code, acptNo);
//            }
//        }
    }

    /**
     * 취득 테이블의 데이터를 적당히 합친다.
     * 일반취득자금데이터에 자기자금, 차입금 데이터를 끼워넣음.
     *
     * @param takings
     * @return
     */
    private Map<String, Taking> aggregate(List<Map<String, Object>> takings) {
        Map<String, Taking> map = new LinkedHashMap<>();
        findGeneral(map, takings);
        aggregate(map, takings);
        return map;
    }

    private void findGeneral(Map<String, Taking> map, List<Map<String, Object>> takings) {
        for (Map<String, Object> _map : takings) {
            if ("개요".equalsIgnoreCase(findType(_map))) {
                Taking taking = to(_map);
                log.debug("<<< "+taking);
                map.put(taking.findKey(), taking);
            }
        }
    }

    private void aggregate(Map<String, Taking> map, List<Map<String, Object>> takings) {
        for (Map<String, Object> _map : takings) {
            String type = findType(_map);
            if ("개요".equalsIgnoreCase(type) == false) {
                if ("자기자금".equalsIgnoreCase(type)) {
                    String name = findName(_map);
                    String birth = findBirth(_map);
                    String resource = findResource(_map);
                    Taking taking = findTaking(map, name, birth);
                    if (taking != null) {
                        taking.setResource(resource);
                    }

                } else if ("차입금".equalsIgnoreCase(type)) {
                    String name = Maps.getValue(_map, "차입자");
                    String borrower = Maps.getValue(_map, "차입처");
                    String borrowingAmount = Maps.getValue(_map, "차입금액");
                    String borrowingPeriod = Maps.getValue(_map, "차입기간");
                    String collateral = Maps.getValue(_map, "담보내역");

                    Taking taking = findTaking(map, name);
                    if (taking != null) {
                        taking.setBorrowingAmount(borrowingAmount);
                        taking.setBorrower(borrower);
                        taking.setBorrowingPeriod(borrowingPeriod);
                        taking.setCollateral(collateral);
                    }
                } else {

                }

            }
        }
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

    private Taking to(Map<String, Object> _map) {
        Taking taking = new Taking();
        taking.setName(findName(_map));
        taking.setBirth(findBirth(_map));
        taking.setTaking(Maps.findValueOrKeys(_map, "자기자금"));
        taking.setBorrowing(Maps.findValueOrKeys(_map, "차입금"));
        taking.setEtc(Maps.findValueOrKeys(_map, "기타"));
        taking.setSum(Maps.findValueOrKeys(_map, "계"));

        return taking;
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
