package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Change;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.StockChangeSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        log.info("<<< "+key);

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

        TableParser stockChangeTableParser = new TableParser(new StockChangeSelectorByPattern());
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
            Table table = (Table) stockChangeTableParser.parse(docRaw, docUrl);
            if (table != null) {
                table.findStock(docUrl, docNo, docNm, FILTER, tnsDt, rptNm,acptNo);
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
            } else {
            }


            for (String _key : FILTER.keySet()) {
                Change change = FILTER.get(_key);
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
            log.info("주식거래내역 ... " + changes.size());
            for (Change change : changes) {
                if (StringUtils.isEmpty(change.getName()) || "-".equalsIgnoreCase(change.getName())) {
                    log.info("거래내역에 이름이 없어 SKIP " + change);
                } else {

                    // 유일한 값이라고 할만한 조회...
                    //mybatis 처리시 paramMap을 다른 클래스에서 처리한 것은 나중에 수정..
                    if (findStockHolder(templateMapper, code, change)) {
                        // 정정된 것은 이미 삭제가 되었으므로 업데이트하지 않아도 딱히 문제가 없음.
                        log.info("ALREADY StockHodler " + change);
                    } else {
                        log.info("INSERT ... " + change);
                        insertStockHolder(templateMapper, change.paramStockHolder(code,acptNo));
                    }
                }
            }

        } else {
            log.error("기초공시가 없음  docNo=" + docNo+" code="+code+" acptNo="+acptNo);
        }


        log.info("done " + key);
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
    private String findBeforeKongsi(TemplateMapper templateMapper, String code, String acptNo) {
        List<Map<String, Object>> maps = templateMapper.findBeforeKongsi(code, acptNo);
        for (Map<String, Object> map : maps) {
            String name = Maps.getValue(map, "doc_nm");
            if (name.contains("정정")) {
                continue;
            } else {
                return Maps.getValue(map, "doc_no");
            }
        }

        return "";
    }


}
