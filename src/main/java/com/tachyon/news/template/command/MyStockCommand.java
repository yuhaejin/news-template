package com.tachyon.news.template.command;

import com.tachyon.article.MyStock;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.MyStockParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.WholeSelector;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.Tables;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class MyStockCommand extends BasicCommand {
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

    private static String STOCK_TYPE = "MYSTOCK";

    @Override
    public void execute(Message message) throws Exception {
        try {

            String key = myContext.findInputValue(message);
            log.info("... " + key);
            if (isSemiRealTimeData(message) == false) {
//                return;
            }

            String[] keys = StringUtils.split(key, "_");
            if (keys.length == 3) {
                String docNo = keys[0];
                String code = keys[1];
                String acptNo = keys[2];
                handleMyStock(docNo, code, acptNo, key);
            }


            log.info("done " + key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public String findArticleType() {
        return "MYSTOCK";
    }

    private void handleMyStock(String docNo, String code, String acptNo, String key) {
        try {
            log.info("... " + key);
            Map<String, Object> map = templateMapper.findKongsiHolder2(docNo, code, acptNo);
            if (map == null) {
                log.info("????????? ??????. " + key);
                return;
            }

            String docUrl = findDocUrl(map);
            String docNm = findDocNm(map);
            if (isOldAtCorrectedKongsi(map)) {
                return;
            }

            if (docNm.contains("????????????") == false) {
                return;
            }
            if (docNm.contains("???????????????")) {
                return;
            }

            String type = findType(docNm);
            if ("no".equalsIgnoreCase(type)) {
                return;
            }
            log.info("?????? ?????? ??????.. "+key+" "+findAcptNoUrl(acptNo));
            String html = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, retryTemplate, loadBalancerCommandHelper);
            WholeSelector selector = new WholeSelector();
            TableParser tableParser = new TableParser(selector);
            List<Table> tables = tableParser.parseSome(html, docUrl, new MyStockParser());

            if (tables.size() == 0) {
                log.info("????????????????????? ??????.. " + docNm + " " + docUrl);
                return;
            }


            int companyIndex = findCompanyIndex(tables);
            Map<String, Object> myStockMap = null;
            String name = null;
            if (companyIndex == -1) {
                name = Maps.getValue(map, "submit_nm");
                myStockMap = Tables.wideOneLineTableToMap(tables.get(0).getBodies(), true, null);
            } else {
                Map<String, Object> companyMap = Tables.widTableToMap(tables.get(companyIndex).getBodies(), true, null);
                name = findCompany(companyMap);
                myStockMap = Tables.wideOneLineTableToMap(tables.get(companyIndex + 1).getBodies(), true, null);
            }

            // ????????? ??????..
            log.debug(myStockMap.toString());
            MyStock myStock = convertToMyStock(type, name, docNm, myStockMap);
            if (myStock != null) {
                if (docNm.contains("??????")) {
                    List<String> _docNos = findBeforeKongsi(templateMapper, docNo, code, acptNo);
                    for (String _docNo : _docNos) {
                        deleteBeforeMyStockHolder(templateMapper,_docNo,code);
                        log.info("??????MyStockHolder ?????? code=" + code + " docNo=" + _docNo);
                        deleteBeforeArticle(templateMapper, _docNo, code,acptNo, findArticleType());
                    }
                }
                myStock.setDocNo(docNo);
                myStock.setAcptNo(acptNo);
                myStock.setIsuCd(code);
                myStock.setAcptDt(acptNo.substring(0, 8));
                handleMyStockDb(myStock, type, docUrl, docNm);
            }

        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    /**
     * ?????? ???????????? ?????? ????????? ?????????????????? ?????? ??????...
     *
     * @param myStock
     * @param type
     * @param docUrl
     */
    private void handleMyStockDb(MyStock myStock, String type, String docUrl, String docNm) {
        log.info("type=" + type + "docUrl=" + docUrl + " " + myStock.toString());
        myStock.setType(type);

        Map<String, Object> findParam = findParam(myStock.getDocNo(), myStock.getIsuCd(), myStock.getAcptNo());
        if (templateMapper.findMyStockCount(findParam) == 0) {
            Map<String, Object> param = insertParam(myStock);
            templateMapper.insertMyStock(param);
            if (isGoodArticle(docNm)) {
                sendToArticleQueue(rabbitTemplate, findPk(param), findArticleType(), findParam);
            }
        } else {
            log.info("SkIP ????????? ?????????.. " + myStock);
        }
    }


    private void deleteBeforeMyStockHolder(TemplateMapper templateMapper, String docNo,String code ) {
        templateMapper.deleteBeforeMyStockHolder(code, docNo);
    }

    private Map<String, Object> insertParam(MyStock myStock) {
        Map<String, Object> param = new HashMap<>();
        param.put("doc_no", myStock.getDocNo());
        param.put("isu_cd", myStock.getIsuCd());
        param.put("acpt_no", myStock.getAcptNo());
        param.put("acpt_dt", myStock.getAcptDt());
        param.put("company", myStock.getCompany());
        param.put("stock_count_gen", myStock.getStockCountGen());
        param.put("stock_count_etc", myStock.getStockCountEtc());
        param.put("stock_amount_gen", myStock.getStockAmountGen());
        param.put("stock_amount_etc", myStock.getStockAmountEtc());
        param.put("start_period", myStock.getStartPeriod());
        param.put("end_period", myStock.getEndPeriod());
        param.put("start_period2", myStock.getStartPeriod2());
        param.put("end_period2", myStock.getEndPeriod2());
        param.put("purpose", myStock.getPurpose());
        param.put("method", myStock.getMethod());
        param.put("dispose_method1", myStock.getDisposeMethod1());
        param.put("dispose_method2", myStock.getDisposeMethod2());
        param.put("dispose_method3", myStock.getDisposeMethod3());
        param.put("dispose_method4", myStock.getDisposeMethod4());
        param.put("before_my_gen_count", myStock.getBeforeMyGenCount());
        param.put("before_my_gen_ratio", myStock.getBeforeMyGenRatio());
        param.put("before_my_etc_count", myStock.getBeforeMyEtcCount());
        param.put("before_my_etc_ratio", myStock.getBeforeMyEtcRatio());
        param.put("before_etc_gen_count", myStock.getBeforeEtcGenCount());
        param.put("before_etc_gen_ratio", myStock.getBeforeEtcGenRatio());
        param.put("before_etc_etc_count", myStock.getBeforeEtcEtcCount());
        param.put("before_etc_etc_ratio", myStock.getBeforeEtcEtcRatio());
        param.put("decision_date", myStock.getDecisionDate());
        param.put("agency", myStock.getAgency());
        param.put("target_stock_gen_price", myStock.getTargetStockGenPrice());
        param.put("target_stock_etc_price", myStock.getTargetStockEtcPrice());
        param.put("type", myStock.getType());
        param.put("amount_unit", myStock.getAmountUnit());
        param.put("stock_unit", myStock.getStockUnit());
        return param;
    }

    /**
     * ???????????? ?????? ????????? ?????? ???????????? ????????? ????????? db??? ????????????.
     * ?????????????????? ?????? ???????????? ??????????????? ?????? ????????? ?????? ?????? ???????????? ?????????.
     * @param type       GN DSPSL CCL
     * @param name       ?????????
     * @param docNm      ?????????
     * @param myStockMap ????????? ?????????.
     */
    private MyStock convertToMyStock(String type, String name, String docNm, Map<String, Object> myStockMap) {
        MyStock myStock = new MyStock();
        myStock.setCompany(name);
        if ("GN".equalsIgnoreCase(type)) {
            if (docNm.contains("??????")) {
                myStock.setupAsTrustGain(myStockMap);
            } else {
                myStock.setupAsGain(myStockMap);
            }
        } else if ("DSPSL".equalsIgnoreCase(type)) {
            myStock.setupAsDisposal(myStockMap);
        } else if ("CCL".equalsIgnoreCase(type)) {
            myStock.setupAsCancel(myStockMap);
        }

        return myStock;
    }

    private String findCompany(Map<String, Object> map) {
        String name = Maps.findValue(map, "?????????");
        return name.trim();
    }

    private String findType(String docNm) {
        if (docNm.contains("??????")) {
            return "CCL";
        } else if (docNm.contains("??????")) {
            return "DSPSL";
        } else if (docNm.contains("??????")) {
            return "GN";
        } else {
            return "NO";
        }
    }

    private int findCompanyIndex(List<Table> tables) {
        for (int i = 0; i < tables.size(); i++) {
            Table table = tables.get(i);
            List<List<String>> lists = table.getBodies();
            if (hasCompany(lists)) {
                return i;
            }
        }

        return -1;
    }

    private boolean hasCompany(List<List<String>> lists) {
        for (List<String> list : lists) {
            String s = toString(list);
            log.info(s);
            if (s.contains("?????????")) {
                return true;
            }
        }
        return false;
    }

    private String toString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (String s : list) {
            sb.append(StringUtils.remove(s, " ")).append(" ");
        }
        return sb.toString();
    }
}
