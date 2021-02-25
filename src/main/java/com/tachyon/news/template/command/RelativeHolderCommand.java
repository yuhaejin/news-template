package com.tachyon.news.template.command;

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
            if (isSemiRealTimeData(message) == false) {
                log.info("준실시간데이터가 아님.. " + key);
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
                log.info("대상 공시가 아님.. " + key);
                return;
            }

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
            if (relatives.size() > 0) {
                for (Relative relative : relatives) {
                    if (isRelative(relative)) {
                        relativeList.add(relative);
                    } else {
                        log.info("친인척이 아님.. " + relative);
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

// 텔레그램 처리하지 않음.
//                    for (Relative relative : relativeList) {
//                        if (hasNewHolder) {
////                        if (isNewRelative(templateMapper, relative)) {
//                            // 실시간공지..
////                            int count = templateMapper.findTelegramHolder(docNo, acptNo, keyword);
////                            if (count == 0) {
////                                templateMapper.insertTelegramHolder(docNo, relative.getCode(), acptNo, keyword);
////                                break;
////                                // 하나만 집어넣어도 됨. 이후 TelegramHelper에서 친인척 데이터를 가져와 처리함.
////                            }
//                        }
//
//                    }

                }

            } else {
                log.warn("친인척 데이터가 없음. " + key);
            }


            log.info("done " + key);


        } catch (Exception e) {
            log.error(e.getMessage(), e);
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
    }

    private boolean isStockChangeKongsi(String docNm) {
        return docNm.contains("주식등의대량보유상황보고서");
    }
}
