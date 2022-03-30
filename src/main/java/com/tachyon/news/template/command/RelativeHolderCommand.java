package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Relative;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.LargestStockHolderParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.RelativeHtmlFinder;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByElement;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 친인척 처리..
 */
@Slf4j
@Component
public class RelativeHolderCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;

    @Autowired
    private RetryTemplate retryTemplate;


    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    private String keyword = "RELATIVE";

    @Override
    public void execute(Message message) throws Exception {
        try {
            String key = myContext.findInputValue(message);
            log.info("... " + key);

            String[] strings = StringUtils.splitByWholeSeparator(key, "_");
            if (strings.length != 3) {
                log.info("... can't parse " + key);
                return;
            }

            String code = strings[1];
            String docNo = strings[0];
            String acptNo = strings[2];

            Map<String, Object> map = findKongsiHalder(templateMapper, docNo, code, acptNo);
            if (map == null) {
                log.error("기초공시가 없음.. " + key);
                return;
            }

            if (isOldAtCorrectedKongsi(map)) {
                log.info("SKIP 정정공시중에 이전공시임. .. " + key);
                return;
            }


            log.info(key + " " + map.toString());
            String docNm = Maps.getValue(map, "doc_nm");
            String docUrl = Maps.getValue(map, "doc_url");
            String acptNm = Maps.getValue(map, "rpt_nm");

            if (docNm.contains("최대주주등소유주식변동신고서") == false && docNm.contains("주식등의대량보유상황보고서") == false) {
                return;
            }

            log.info("친인척 처리중.. "+key+" "+findAcptNoUrl(acptNo));
            String docRaw = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, retryTemplate, loadBalancerCommandHelper);

            List<Relative> relatives = new ArrayList<>();
            if (isMajorStockChangeKongis(docNm)) {
                StockChangeSelectorByElement selector = new StockChangeSelectorByElement();
                selector.setKeywords(new String[]{"최대", "주주", "주식", "소유", "현황"});

                TableParser stockChangeTableParser = new TableParser(selector);
                LargestStockHolderParser myParser = new LargestStockHolderParser();
                myParser.setStrings(new String[]{"주식수", "비율"});

                List<Table> tables = stockChangeTableParser.parseSome(docRaw, docUrl, myParser);
                if (tables != null && tables.size() > 0) {
                    for (Table _table : tables) {
                        _table.findRelative(relatives, docNo, code, acptNo, docUrl);
                    }
                }
            } else if (isStockChangeKongsi(docNm)) {
                StockChangeSelectorByPattern selector = new StockChangeSelectorByPattern();
                selector.setKeywords(new String[]{"대량", "보유자", "사항"});
                selector.setHtmlFinder(new RelativeHtmlFinder());
                TableParser stockChangeTableParser = new TableParser(selector);
                Table table = (Table) stockChangeTableParser.parse(docRaw, docUrl);
                if (table != null) {
                    table.findRelative(relatives, docNo, code, acptNo, docUrl);


                }
            } else {
                //do nothing..
            }

            List<Relative> relativeList = new ArrayList<>();
            List<Relative> staffList = new ArrayList<>();
            if (relatives.size() > 0) {
                for (Relative relative : relatives) {
                    if (isRelative(relative)) {
                        relativeList.add(relative);
                    } else {
                        String relation = relative.getRelationWithHim();
                        if ("공동보유자".equalsIgnoreCase(relation)) {
                            staffList.add(relative);
                        } else {
                            log.info("친인척이 아님.. " + relative);
                        }
                    }
                }
                log.info("친인척갯수 " + relativeList.size());
                if (relativeList.size() > 0) {
                    if (isChangeKongsi(acptNm)) {
                        List<String> _docNos = findBeforeKongsi(templateMapper, docNo, code, acptNo);
                        for (String _docNo : _docNos) {

                            log.info("이전RelativeHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);
                            deleteBeforeRelativeHolder(templateMapper, _docNo, code);
                            log.info("이전RelativeHolder 삭제 code=" + code + " docNo=" + _docNo);
                        }
                    }

                    boolean hasNewHolder = false;
                    for (Relative relative : relativeList) {
                        Map<String, Object> param = relative.paramMap();
                        if (findRelativeHolderCount(templateMapper, param) == 0) {
                            log.info(param.toString());
                            if (isNewHolder(templateMapper, param)) {
                                param.put("new_yn", "Y");
                                hasNewHolder = true;
                            } else {
                                param.put("new_yn", "N");
                            }
                            templateMapper.insertRelativeHolder(param);
                        } else {
                            log.info("SKIP 중복된 데이터 " + param);
                        }
                    }
                }

                if (staffList.size() > 0) {
                    for (Relative relative : staffList) {
                        // 정정시 처리X
                        // 임원에 처리. 중복여부 확인후 INSERT 해보자.
                        log.info(".. "+relative);
                        Map<String, Object> staffParam = staffParam(relative);
                        List<Map<String, Object>> maps = templateMapper.findSimpleStaffHolder(staffParam);
                        if (maps == null || maps.size() == 0) {
                            templateMapper.insertStaffHolder(staffParam);
                            log.info("INSERT 임원 "+staffParam);
                        } else {
                            log.info("SKIP 임원 중복됨 "+staffParam);
                        }
                    }
                }


            } else {
                log.warn("친인척 데이터가 없음. " + key);
            }


            log.info("done " + key);


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private Map<String, Object> staffParam(Relative relative) {
        Map<String, Object> map = new HashMap<>();
        map.put("kongsi_day", relative.getAcptNo().substring(0,8));
        map.put("name", relative.getName());
        map.put("gender", "");
        map.put("birth_day", BizUtils.convertBirth(relative.getBirth(),relative.getAcptNo().substring(0,8)));   //생년월일 => 년월 로 변경
        map.put("spot", "공동보유자");
        map.put("regi_officer_yn", "N");
        map.put("full_time_yn", "N");
        map.put("responsibilities", relative.getRelationWithCom());
        map.put("major_career", "-");
        map.put("birth_ym", toBirthYm(relative.getBirth()));
        map.put("voting_stock", 0);
        map.put("no_voting_stock", 0);
        map.put("service_duration", "");
        map.put("expiration_date", "");
        map.put("isu_cd", relative.getCode());
        map.put("doc_no", relative.getDocNo());
        map.put("doc_url", relative.getDocUrl());
        map.put("acpt_no", relative.getAcptNo());

        return map;
    }

    /**
     *
     * @param birth
     * @return
     */
    private String toBirthYm(String birth) {
        if (isEmpty(birth)) {
            return "";
        } else {
            if (birth.length() >= 4) {
                return birth.substring(0, 4);
            } else {
                return birth;
            }
        }
    }


    @Override
    public String findArticleType() {
        return "";
    }

    private boolean isNewHolder(TemplateMapper templateMapper, Map<String, Object> param) {
        int size = templateMapper.findRelativeHodlerSize(param);
        if (size > 0) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * TODO 구현해야 함.
     *
     * @param templateMapper
     * @param relative
     * @return
     */
    private boolean isNewRelative(TemplateMapper templateMapper, Relative relative) {
        return false;
    }

    private int findRelativeHolderCount(TemplateMapper templateMapper, Map<String, Object> param) {
        return templateMapper.findRelativeHolderCount(param);
    }

    private void deleteBeforeRelativeHolder(TemplateMapper templateMapper, String docNo, String code) {
        templateMapper.deleteBeforeRelativeHolder(code, docNo);
    }

    private boolean isRelative(Relative relative) {
        String relation = relative.getRelationWithHim();
        return StringUtils.containsAny(relation, "친척", "인척", "본인");
//        return StringUtils.containsAny(relation, "친척", "인척", "본인","공동보유자");
    }

    private boolean isStockChangeKongsi(String docNm) {
        return docNm.contains("주식등의대량보유상황보고서");
    }
}
