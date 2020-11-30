package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Purpose;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.StaffReportParser;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.StaffReporterSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.crawl.kind.util.Tables;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 임원공시에는 아직 나와 있지 않지만
 * 주식취득을 공시하는 임원에 대해서
 * 그 정보를 스크래핑하여 처리한다.
 *
 * 선임일에 딱 한번
 * 퇴임일에 딱 한번만 테이블에 들어가도록 한다.
 *
 */
@Slf4j
@Component
public class StaffReportCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;

    @Autowired
    private TemplateMapper templateMapper;

    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;
    @Autowired
    private RetryTemplate retryTemplate;
    @Override
    public void execute(Message message) throws Exception {
        String key = myContext.findInputValue(message);
        log.info("<<< "+key);
        String[] keys = StringUtils.split(key, "_");
        String docNo = keys[0];
        String code = keys[1];
        String acptNo = keys[2];
        Map<String, Object> kongsiHolder = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
        if (kongsiHolder == null) {
            log.error("기초공시가 없음 docNo=" + docNo + " code=" + code + " acptNo=" + acptNo);
            return;
        }

        if (isOldAtCorrectedKongsi(kongsiHolder)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }


        String docNm = Maps.getValue(kongsiHolder, "doc_nm");
        if (docNm.contains("임원")==false) {
            log.info("임원 키워드가 없는 공시임. docNo=" + docNo + " code=" + code + " acptNo=" + acptNo+" "+docNm);
            return;
        }

        String docUrl = Maps.getValue(kongsiHolder, "doc_url");
        String c = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, retryTemplate,loadBalancerCommandHelper);

        TableParser parser = new TableParser(new StaffReporterSelectorByPattern());
        List<Table> tables = parser.parseSome(c , docUrl,new StaffReportParser());
        if (tables != null && tables.size() > 0) {
            // 테이블은 하나이며 보고자도 한명이다.
            Table table = tables.get(0);
            Map<String, Object> map = Tables.widTableToMap(table);
            log.debug(map.toString());
            String name = Maps.findValueAndKeys(map,"성명","한글");
            String homeabroad = Maps.findValueAndKeys(map,"보고자","구분");
            String address = Maps.findValueAndKeys(map,"주소");
            String birth = Maps.findValueOrKeys(map,"생년월일","주민등록번호");
            String relationHim = modify(Maps.findValueAndKeys(map, "회사", "직위"));
            String relationCom = modify(Maps.findValueAndKeys(map, "회사", "주요주주"));
            String registeredStaff = modify(Maps.findValueAndKeys(map, "회사", "등기여부"));
            String startDate = modify(BizUtils.parseSimpleDay(Maps.findValueAndKeys(map, "회사", "선임일")));
            String endDate = modify(BizUtils.parseSimpleDay(Maps.findValueAndKeys(map, "회사", "퇴임일")));

            log.debug("name "+name);
            log.debug("homeabroad "+homeabroad);
            log.debug("address "+address);
            log.debug("birth "+birth);
            log.debug("relationHim "+relationHim);
            log.debug("relationCom "+relationCom);
            log.debug("registeredStaff "+registeredStaff);
            log.debug("startDate "+startDate);
            log.debug("endDate "+endDate);



            if (isEmpty(startDate)) {
                log.info("선임일이 없는 보고자는 SKIP "+name+" "+docUrl);
                return;
            }

            /**
             * 1. 정정공시인 경우 이전 데이터 삭제
             * 2. 선임일이 있는 경우
             *  - 해당 임원이 있는지 확인하여 없으면 입력
             *  - 있으면 SKIP
             *  3. 퇴임일이 있는 경우
             *  - 해당 정보가 없으면 입력
             *  - 있으면 SKIP
             *
             */

            if (docNm.contains("정정")) {
                String _docNo = findBeforeKongsi(templateMapper, code, acptNo);
                if (StringUtils.isEmpty(_docNo) == false) {
                    if (docNo.equalsIgnoreCase(_docNo) == false) {
                        deleteBeforeStaffHolder(templateMapper, code, _docNo);
                        log.info("이전 StaffHolder 삭제 code=" + code + " docNo=" + _docNo);
                        deleteBeforeArticle(templateMapper,_docNo,acptNo,code);
                    }
                }
            }


            String _birth = BizUtils.convertBirth(birth,acptNo.substring(0,8));
            log.debug(birth +" ==> "+_birth);
            if (hasStaff(templateMapper,code, name, _birth) == false) {
                Map<String, Object> param = params(startDate,name,_birth,relationHim,registeredStaff,code,docNo,docUrl,acptNo,relationCom);
                insertNewStaff(templateMapper,param);
            }
            if (isEmpty(endDate) == false) {
                if(hasStaffLastDay(templateMapper,code,name,_birth,endDate)==false){
                    Map<String, Object> param = params(endDate,name,_birth,"임원퇴임","",code,docNo,docUrl,acptNo,"");
                    insertRetiredStaff(templateMapper, param);
                }
            }

        } else {
        }

        log.info("done "+key);
    }

    /**
     * 퇴임한 임원 처리
     * 아래 새로운 임원처리와 같은 메소드 호출
     * @param templateMapper
     * @param param
     */
    private void insertRetiredStaff(TemplateMapper templateMapper, Map<String, Object> param) {
        log.info("insertRetiredStaff "+param.toString());
        templateMapper.insertStaffHolder(param);
    }

    private boolean hasStaffLastDay(TemplateMapper templateMapper, String code, String name, String birth, String endDate) {
        if (templateMapper.findLastDayStaffCount(code, name, birth, endDate) > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 새로운 임원 처리
     * @param templateMapper
     * @param param
     */
    private void insertNewStaff(TemplateMapper templateMapper,Map<String, Object> param) {
        log.info("insertNewStaff "+param.toString());
        templateMapper.insertStaffHolder(param);
    }

    /**
     * #{kongsi_day}, #{name}, #{gender}, #{birth_day}, #{spot}, #{regi_officer_yn}, #{full_time_yn}, #{responsibilities},
     * #{major_career}, #{voting_stock}, #{no_voting_stock}, #{service_duration}, #{expiration_date}, #{isu_cd}, #{doc_no},
     *                 #{doc_url}, #{acpt_no}
     * @return
     * @param startDate
     * @param name
     * @param birth
     * @param relationHim
     */
    private Map<String, Object> params(String startDate, String name, String birth, String relationHim, String registeredStaff, String isuCd, String docNo, String docUrl, String acptNo, String responsibilities) {
        Map<String, Object> map = new HashMap<>();
        map.put("kongsi_day", startDate);
        map.put("name", name);
        map.put("gender", "");
        map.put("birth_day", birth);
        map.put("spot", relationHim);
        map.put("regi_officer_yn", isRegisteredStaff(registeredStaff));
        map.put("full_time_yn", "Y");
        map.put("responsibilities", responsibilities);
        map.put("major_career", "");
        map.put("voting_stock", "0");
        map.put("no_voting_stock", "0");
        map.put("service_duration", "");
        map.put("expiration_date", "");
        map.put("isu_cd", isuCd);
        map.put("doc_no", docNo);
        map.put("doc_url", docUrl);
        map.put("acpt_no", acptNo);
        return map;
    }

    private String isRegisteredStaff(String value) {
        if (value.contains("비등기")) {
            return "N";
        } else if (value.contains("등기")) {
            return "Y";
        } else {
            return "N";

        }
    }
    /**
     * 회사, 이름, 생일로 확인.
     * @param code
     * @param name
     * @param birth yyyyMM 형식임.
     * @return
     */
    private boolean hasStaff(TemplateMapper templateMapper,String code, String name, String birth) {

        int count = templateMapper.findStaffCount(code, name, birth);
        if (count > 0) {
            return true;
        } else {
            return false;
        }
    }


    private void deleteBeforeStaffHolder(TemplateMapper templateMapper, String code, String docNo) {
        templateMapper.deleteBeforeStaffHolder(code, docNo);
    }

    private String modify(String value) {
        value = value.trim();
        value = StringUtils.remove(value, "-");
        value = StringUtils.remove(value, " ");
        return value;
    }


    private void handleDb(Purpose purpose) {
        Map<String, Object> param = purpose.findParam();
        int count = templateMapper.findPurposeHolder(param);
        if (count == 0) {
            if (BizUtils.isEmpty(purpose.getContents())) {
                templateMapper.insertPurposeHolder2(purpose.insertParam());
            } else {
                templateMapper.insertPurposeHolder1(purpose.insertParam());
            }
        } else {
            log.info("이미처리됨 "+purpose.getCode()+" "+purpose.getDocNo()+" "+purpose.getDocUrl());
        }
    }
}
