package com.tachyon.news.template.command;

import com.tachyon.article.SupplyContract;
import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.MyContractParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.WholeSelector;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.Tables;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 계약체결, 해지 처리
 */
@Slf4j
@Component
public class ContractCommand extends BasicCommand {
    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private MyContext myContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Override
    public void execute(Message message) throws Exception {

        String key = myContext.findInputValue(message);
        log.info("... " + key);
        DocNoIsuCd docNoIsuCd = find(key);
        if (docNoIsuCd == null) {
            log.info("... can't parse " + key);
            return;
        }

        String docNo = docNoIsuCd.getDocNo();
        String code = docNoIsuCd.getIsuCd();
        String acptNo = docNoIsuCd.getAcptNo();

        Map<String, Object> kongsi = templateMapper.findKongsiHalder2(docNo, code, acptNo);
        if (kongsi == null) {
            log.warn("공시가 없음..  " + key);
            return;
        }
        String docUrl = Maps.getValue(kongsi, "doc_url");
        String docNm = Maps.getValue(kongsi, "doc_nm");
        if (docNm.contains("단일판매")==false) {
            log.warn("계약 관련 공시가 아님.  " + key+" "+docUrl);
            return;
        }

        String path = findPath(myContext.getHtmlTargetPath(), code, docNo, "htm");
        File f = new File(path);
        if (f.exists() == false) {
            log.warn("파일이 없음. " + path + " " + key);
            return;
        }

        if (isOldAtCorrectedKongsi(kongsi)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }

        String company = Maps.getValue(kongsi, "submit_nm");
        String c = FileUtils.readFileToString(f, "UTF-8");
        WholeSelector selector = new WholeSelector();
        TableParser tableParser = new TableParser(selector);
        List<Table> tables = tableParser.parseSome(c, docUrl, new MyContractParser());
        if (tables.size() != 1) {
            log.warn("계약테이블이 없음. "+key+" "+docUrl);
            return;
        }

        Map<String, Object> map = Tables.wideOneLineTableToMap(tables.get(0).getBodies(), true, null);
        if (map.size() == 0) {
            log.warn("계약테이블이 없음. "+key+" "+docUrl);
            return;
        }

        SupplyContract supplyContract = SupplyContract.from(map,docNm,company,docNo,code,acptNo);
        log.info(supplyContract.toString());
        if (supplyContract.validate() == false) {
            log.warn("계약테이블을 분석할 수 없음 "+key+" "+docUrl);
            return;
        }

        if (docNm.contains("정정")) {
            // 이전 공시 찾아 삭제처리..
            String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
            log.info("이전 ContractHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);
            if (StringUtils.isEmpty(_docNo) == false) {
                if (docNo.equalsIgnoreCase(_docNo) == false) {
                    deleteBeforeContractHolder(templateMapper, code, _docNo);
                    log.info("이전 ContractHolder 삭제 code=" + code + " docNo=" + _docNo);
                }
            }
        }


        Map<String, Object> findParam = findParam(supplyContract, code);
        int count = templateMapper.findSupplyContractCount(findParam);
        if (count == 0) {
            Map<String, Object> param = param(supplyContract);
            log.info("INSERT "+param);
            templateMapper.insertSupplyContract(param);
            sendToArticleQueue(rabbitTemplate,findPk(param),"CONTRACT",findParam);
        } else {
            log.info("이미 존재하는 계약,, "+key);
        }
    }
    private Map<String,Object> findParam(SupplyContract supplyContract,String code) {
        Map<String, Object> map = new HashMap<>();
        map.put("contract_gubun", supplyContract.getGubun());
        map.put("isu_cd", code);
        map.put("action_type", supplyContract.getActionType());
        map.put("do_date", supplyContract.getDoDate());
        return map;
    }
    private Map<String, Object> param(SupplyContract supplyContract) {
        Map<String, Object> map = new HashMap<>();
        map.put("action_type", supplyContract.getActionType());
        map.put("contract_gubun",supplyContract.getGubun());
        map.put("contract_name",supplyContract.getContractName());
        map.put("contents1",supplyContract.getContents1());
        map.put("contents2",supplyContract.getContents2());
        map.put("contents3",supplyContract.getContents3());
        map.put("contents4",supplyContract.getContents4());
        map.put("contents5",supplyContract.getContents5());
        map.put("contents6",supplyContract.getContents6());
        map.put("contents7",supplyContract.getContents7());
        map.put("target",supplyContract.getTarget());
        map.put("relation_with_company",supplyContract.getRelationWithCompany());
        map.put("sales_area",supplyContract.getSalesArea());
        map.put("contract_start_date",modifyToDate(supplyContract.getContractStartDate()));
        map.put("contract_end_date",modifyToDate(supplyContract.getContractEndDate()));
        map.put("contract_condition",supplyContract.getContractCondition());
        map.put("termination_reason",supplyContract.getTerminationReason());
        map.put("do_date",modifyToDate(supplyContract.getDoDate()));
        map.put("reservation_reason",supplyContract.getReservationReason());
        map.put("reservation_period",supplyContract.getReservationPeriod());
        map.put("etc",modifyEtc(supplyContract.getEtc()));
        map.put("doc_no",supplyContract.getDocNo());
        map.put("isu_cd",supplyContract.getIsuCd());
        map.put("acpt_no",supplyContract.getAcptNo());
        map.put("acpt_dt",supplyContract.getAcptNo().substring(0,8));
        map.put("contract_date",supplyContract.getContractDate());

        return map;
    }
    private String modifyEtc(String etc) {
        return StringUtils.rightPad(etc, 400);
    }

    private String modifyToDate(String s) {
        String[] strings = BizUtils.findNumeric(s);
        if (strings.length == 3) {
            String y = strings[0];
            String m = strings[1];
            if (m.length() == 1) {
                m = "0" + m;
            }
            String d = strings[2];
            if (d.length() == 1) {
                d = "0" + d;
            }
            return y + m + d;

        } else {
            return s;
        }

    }
    private void deleteBeforeContractHolder(TemplateMapper templateMapper,  String docNo,String code) {
        templateMapper.deleteBeforeContractHolder(docNo, code);
    }
}
