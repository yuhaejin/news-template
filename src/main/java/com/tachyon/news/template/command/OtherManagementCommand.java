package com.tachyon.news.template.command;

import com.tachyon.article.Touch;
import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.EtcKongsiParser;
import com.tachyon.crawl.kind.parser.MyTouchParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.WholeSelector;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.Tables;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import sun.util.resources.cldr.am.CurrencyNames_am;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 기타 경영 공시 처리.
 */
@Slf4j
@Component
public class OtherManagementCommand extends BasicCommand {

    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private MyContext myContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private RetryTemplate retryTemplate;
    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Override
    public void execute(Message message) throws Exception {
        try {
            String key = myContext.findInputValue(message);
            DocNoIsuCd docNoIsuCd = find(key);
            if (docNoIsuCd == null) {
                log.info("... can't parse " + key);
                return;
            }

            String docNo = docNoIsuCd.getDocNo();
            String code = docNoIsuCd.getIsuCd();
            String acptNo = docNoIsuCd.getAcptNo();
            TableParser parser = new TableParser(new WholeSelector());

            handleOtherManagement(docNo, code, acptNo, key, parser);

            log.info("done " + key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void handleOtherManagement(String docNo, String code, String acptNo, String key, TableParser tableParser) throws Exception {

        Map<String, Object> _map = templateMapper.findKongsiHolder2(docNo, code, acptNo);
        if (_map == null) {
            log.info("공시가 없음. " + key);
            return;
        }

        if (isOldAtCorrectedKongsi(_map)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }

        String docUrl = findDocUrl(_map);
        String docNm = findDocNm(_map);

        if (Maps.hasAndKey(docNm, "기타", "경영사항") == false) {
            log.info("기타 경영사항 공시가 아님. " + key + " " + docUrl);
            return;
        }
        String filePath = filePath(myContext.getHtmlTargetPath(), docNo, code);
        log.info("... " + key + " " + docUrl + " " + filePath);
        String html = findDocRow(filePath, docUrl, retryTemplate, loadBalancerCommandHelper);
        if (StringUtils.isEmpty(html)) {
            log.info("공시데이터가 없음. " + key + " " + docUrl);
            return;
        }

        EtcKongsiParser myParser = new EtcKongsiParser();
        List<Map<String, Object>> tables = (List<Map<String, Object>>) tableParser.simpleParse(html, docUrl, myParser);

        if (tables == null || tables.size() == 0) {
            log.info("테이블데이터가 없음.. " + docNm + " " + docUrl);
            return;
        }

        if (docNm.contains("정정")) {
            String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
            if (StringUtils.isEmpty(_docNo) == false) {
                log.info("정정공시중에 이전 공시 삭제... " + _docNo + " " + code);
                templateMapper.deleteBeforeTouchHolder(_docNo, code);
                deleteBeforeArticle(templateMapper,_docNo,acptNo,code);
            }
        }
        Map<String, Object> param = null;
        if (docNm.contains("특허권")) {
            param = patentParam(tables, acptNo, code, docNo);
        } else {
            param = param(tables, acptNo, code, docNo);
        }

        String title = Maps.getValue(param, "title");
        if (title == null || "".equalsIgnoreCase(title.trim())) {
            log.info("제목이 없음. "+docUrl);
            return;
        }

        // code, 타이틀 acptt 로 구분이 가능해 보임..
        if (templateMapper.findEtcManagementCount(param) == 0) {
            log.info("기타경영 처리.. " + param);
            templateMapper.insertEtcManagement(param);
        } else {
            log.info("기타경영 중복.. " + param);
//            templateMapper.updateEtcManagement(param);
        }
    }

    private Map<String, Object> patentParam(List<Map<String, Object>> tables, String acptNo, String code, String docNo) {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> first = findPatent(tables);
        log.debug("기타 경영 특허 " + first);
        map.put("acpt_dt", acptNo.substring(0, 8));
        map.put("doc_no", docNo);
        map.put("isu_cd", code);
        map.put("acpt_no", acptNo);
        map.put("title", Maps.findValueAndKeys(first, "특허","명칭"));
        map.put("contents", Maps.findValueAndKeys(first, "특허","주요내용"));
        map.put("confirm_date", convertDate(Maps.findValueAndKeys(first, "확인", "일자")));
        map.put("etc", Maps.findValueAndKeys(first, "기타","투자판단"));

        map.put("agent", Maps.findValueAndKeys(first, "특허권자"));
        map.put("major_biz", Maps.findValueAndKeys(first, "특허","활용계획"));

        return map;
    }


    private Map<String, Object> param(List<Map<String, Object>> maps, String acptNo, String code, String docNo) {
        Map<String, Object> map = new HashMap<>();
        if (maps.size() == 0) {
            return map;
        }

        Map<String, Object> first = findFirst(maps);
        if (first == null) {
            return map;
        }
        setupNoSub(map, first, acptNo, code, docNo);
        Map<String, Object> second = findSecond(maps);
        if (second == null) {
            return map;
        }
        setupSub(map, second);
        return map;
    }

    private Map<String, Object> findSecond(List<Map<String, Object>> maps) {
        for (Map<String, Object> map : maps) {
            if (Maps.hasAndKey(map, "지배회사","연결","자산총액")) {
                return map;
            }
        }
        return null;
    }
    private Map<String, Object> findMap(List<Map<String, Object>> maps,String... keys) {
        for (Map<String, Object> map : maps) {
            if (Maps.hasAndKey(map, keys)) {
                return map;
            }
        }
        return null;
    }

    private Map<String, Object> findFirst(List<Map<String, Object>> maps) {
        return findMap(maps, "주요내용");
    }
    private Map<String, Object> findPatent(List<Map<String, Object>> maps) {
        return findMap(maps, "특허","명칭");
    }
    private void setupSub(Map<String, Object> map, Map<String, Object> first) {
        log.debug("기타 경영 종속회사 " + first);
        map.put("subsidiary", Maps.findValueAndKeys(first, "종속회사명"));
        map.put("ename", Maps.findValueAndKeys(first, "영문"));
        map.put("agent", Maps.findValueAndKeys(first, "대표자"));
        map.put("major_biz", Maps.findValueAndKeys(first, "주요사업"));
        map.put("major_sub_com_yn", Maps.findValueAndKeys(first, "주요종속회사여부"));
        map.put("total_assets", convertNumber(Maps.findValueAndKeys(first, "종속회사", "자산총액")));
        map.put("ctrl_total_assets", convertNumber(Maps.findValueAndKeys(first, "지배회사", "연결자산")));
        map.put("ctrl_total_assets_ratio", Maps.findValueAndKeys(first, "지배회사", "연결자산", "대비"));
    }

    private void setupNoSub(Map<String, Object> map, Map<String, Object> first, String acptNo, String code, String docNo) {
        log.debug("기타 경영 일반공시 " + first);
        map.put("acpt_dt", acptNo.substring(0, 8));
        map.put("doc_no", docNo);
        map.put("isu_cd", code);
        map.put("acpt_no", acptNo);
        map.put("title", Maps.findValueAndKeys(first, "제목"));
        map.put("contents", Maps.findValueAndKeys(first, "주요내용"));
        map.put("confirm_date", convertDate(Maps.findValueAndKeys(first, "결정", "일자")));
        map.put("etc", findEtc(first));
    }

    private String findEtc(Map<String, Object> first) {
        List<String> strings = new ArrayList<>();
        for (String key : first.keySet()) {
            strings.add(key);
        }

        int index = findEtc(strings);
        if (index == -1) {
            return "";
        }

        if (index == strings.size() - 1) {
            return "";
        }

        String key = strings.get(index + 1);
        return (String) first.get(key);
    }

    private int findEtc(List<String> strings) {
        for (int i = 0; i < strings.size(); i++) {
            String key = strings.get(i);
            if (Maps.hasAndKey(key, "기타", "투자", "참고")) {
                return i;
            }
        }
        return -1;
    }

    private String convertNumber(String number) {
        log.debug("찾은 숫자 " + number);
        String[] strings = BizUtils.findNumeric(number);
        StringBuilder sb = new StringBuilder();
        for (String s : strings) {
            sb.append(s);
        }
        return sb.toString();
    }

    private String convertDate(String confirmDate) {
        String[] strings = BizUtils.findNumeric(confirmDate);
        String y = "";
        String m = "";
        String d = "";
        if (strings.length == 3) {
            y = strings[0];
            m = strings[1];
            d = strings[2];

            if (m.length() == 1) {
                m = "0" + m;
            }
            if (d.length() == 1) {
                d = "0" + d;
            }

            return y + m + d;

        } else {
            StringBuilder sb = new StringBuilder();
            for (String s : strings) {
                sb.append(s);
            }
            return sb.toString();
        }

    }
}
