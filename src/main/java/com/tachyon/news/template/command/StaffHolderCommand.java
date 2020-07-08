package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Staff;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.handler.StaffChangeSelectorByPattern;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class StaffHolderCommand extends BasicCommand {
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

        String docNo = docNoIsuCd.getDocNo();
        String code = docNoIsuCd.getIsuCd();
        String acptNo = docNoIsuCd.getAcptNo();

        Map<String, Object> kongsiHolder = findKongsiHalder(myContext, message, templateMapper, docNo, code, acptNo);
        if (kongsiHolder == null) {
            log.error("기초공시가 없음.. " + key);
            return;
        }

        if (isOldAtCorrectedKongsi(kongsiHolder)) {
            log.info("SKIP 정정공시중에 이전공시임. .. " + key);
            return;
        }


        String docNm = Maps.getValue(kongsiHolder, "doc_nm");
        String rptNm = Maps.getValue(kongsiHolder, "rpt_nm");
        String docUrl = Maps.getValue(kongsiHolder, "doc_url");

        if (docNm.contains("기업실사") || docNm.contains("합병") || docNm.contains("투자설명")) {
            log.warn("SkIP :  공시명 " + docNm + " " + docUrl);
            return;
        }

        String html = findDocRow(myContext.getHtmlTargetPath(), docNo, code, docUrl, loadBalancerCommandHelper);
        if (isEmpty(html)) {
            log.error("공시Html을 수집할 수 없음. " + key + " " + docUrl);
            return;
        }
        TableParser tableParser = new TableParser(new StaffChangeSelectorByPattern());
        SimpleContext simpleContext = new SimpleContext();
        StaffMyParser myParser = new StaffMyParser();
        myParser.setSimpleContext(simpleContext);
        List<Table> tables = tableParser.parseSome(html, docUrl, myParser);
        if (tables == null || tables.size() == 0) {
            log.info("SKIP 임원정보가 없음. " + key + " " + docUrl);
            return;
        }
        if (tables.size() > 0) {
            Map<String, Staff> FILTER = new HashMap<>();
            findStaff(tables, code, docNo, docUrl, acptNo, FILTER, simpleContext);
            double ratio = simpleContext.getRatio();
            if (ratio < 50l) {
                log.error(ratio + " wrong parsing_" + rptNm + "_" + docNm + "_" + docUrl);
            } else {
                log.info(ratio + " parsing_" + rptNm + "_" + docNm + "_" + docUrl);
            }

            setupKongsiDay(FILTER, acptNo);
            setupBirthDay(FILTER, acptNo);
            if (isCorrectBirthday(message)) {
                correctBirthday(FILTER,docNo,code,acptNo);


            } else {
                handleFilter(FILTER, docNo, acptNo);
            }

        } else {
            // 예외상황임...
            log.error("Can't parse StaffTemplate " + code + " " + docNm + " " + docUrl + " " + rptNm);

        }
    }

    /**
     * 생일데이터 보정..
     * @param FILTER
     * @param docNo
     * @param code
     * @param acptNo
     */
    private void correctBirthday(Map<String, Staff> FILTER, String docNo, String code, String acptNo) {
        List<Staff> staffList = templateMapper.findStaffHolder(docNo,code,acptNo);
        for (Staff staff : staffList) {
            log.debug("<< "+staff);
        }
        for (String key : FILTER.keySet()) {
            Staff staff = FILTER.get(key);
            log.debug("분석된임원 "+staff);
            Staff dbStaff = findStaff(staff, staffList);
            if (dbStaff == null) {
                continue;
            }
            String birthDay = staff.getBirthDay();
            if (isEmpty(birthDay)) {
                continue;
            }
            log.debug("DB임원 "+dbStaff);

            if (birthDay.equalsIgnoreCase(dbStaff.getBirthDay())==false) {
                log.info("DIFF "+birthDay+" <== db "+dbStaff.getBirthDay()+" "+staff.getName()+" "+docNo+" "+code+" "+acptNo);
                templateMapper.updateSimpleStockHolderBirthDay(dbStaff.getSeq(),staff.getBirthDay(),staff.getBirthYm());
            }

        }
    }

    /**
     * DB에서 조회된 데이터에서 staff에 해당하는 데이터 추출하기...
     * @param staff 임원 정보
     * @param staffList db에서 조회한 데이터..
     * @return
     */
    private Staff findStaff(Staff staff, List<Staff> staffList) {
        for (Staff _staff : staffList) {
            if (_staff.compare(staff)) {
                return _staff;
            }
        }
        return null;
    }

    private void setupBirthDay(Map<String, Staff> FILTER, String acptNo) {
        for (String key : FILTER.keySet()) {
            Staff staff = FILTER.get(key);
            // 예전 공시는 생년월일까지 있으나 요즘 공시는 생년월까지 있어 아래처럼 작업한다.
            String birthDay = BizUtils.convertBirth(staff.getBirthDay(),acptNo.substring(0,8));
            staff.setBirthDay(birthDay);
            if (isBirthDay(birthDay)) {
                staff.setBirthYm(toStaffYYMM(birthDay));
            } else {
                staff.setBirthYm(birthDay);
            }
        }
    }

    private String toStaffYYMM(String birth) {
        return birth.substring(2);
    }


    boolean isCorrectBirthday(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__BIRTHDAY")) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * 기초공시 acpt_no 을 통해 이전 것이면 처리하지 않는다. 이후 것이면 이전 것은 삭제한다.
     * - 처리는 하나의 공시단위로 처리하면 될 듯...
     *
     * @param FILTER
     */
    private void handleFilter(Map<String, Staff> FILTER, String docNo, String acptNo) {
        boolean doDelete = false;
        for (String key : FILTER.keySet()) {
            Staff staff = FILTER.get(key);
            // acptNo docNo docUrl
            List<Map<String, Object>> maps = templateMapper.findSimpleStaffHolder(staff.toKeyParamMap());
            if (maps == null || maps.size() == 0) {
                //insert...
                log.info("insert staffInfo " + staff);
                templateMapper.insertStaffHolder(staff.toParamMap());
            } else {
                if (maps.size() == 1) {
                    Map<String, Object> map = maps.get(0);
                    String _acptNo = Maps.getValue(map, "acpt_no");
                    if (acptNo.compareTo(_acptNo) > 0) {
                        // delete 후 insert 해야...
                        if (doDelete == false) {
                            String _docNo = Maps.getValue(map, "doc_no");
                            Map<String, Object> deleteParam = deleteParam(_docNo, staff.getIsuCd());
                            log.info("delete 이전 StaffInfo " + deleteParam);
                            templateMapper.deleteBeforeStaffHolder(deleteParam);
                            doDelete = true;
                        }
                        log.info("insert staffInfo " + staff);
                        templateMapper.insertStaffHolder(staff.toParamMap());
                    } else {
                        log.info("SKIP 오래된 StaffInfo " + staff);
                    }
                } else {
                    log.error("조회된임원데이터가 두개이상임.. " + staff.toKeyParamMap());
                }


            }

        }

    }

    /**
     * code,docNo
     *
     * @param docNo
     * @param isuCd
     * @return
     */
    private Map<String, Object> deleteParam(String docNo, String isuCd) {
        Map<String, Object> map = new HashMap<>();
        map.put("isu_cd", isuCd);
        map.put("doc_no", docNo);
        return map;
    }


    private void findStaff(List<Table> tables, String code, String docNo, String docUrl, String acptNo, Map<String, Staff> FILTER, SimpleContext simpleContext) throws Exception {
        String kongsiDay = findTheDay(tables.get(0), docNo);
        boolean hasUnknown = false;
        int count = FILTER.size();
        for (Table table : tables) {
            String title = table.getTitle();
            log.debug("title " + title);
            if ("UNKNOWN".equalsIgnoreCase(title)) {
                hasUnknown = true;
            }
            if ("SKIP".equalsIgnoreCase(title)) {

            } else {
                if (parsibleTitle(title)) {
                    table.findStaff(code, docNo, docUrl, acptNo, kongsiDay, FILTER);
                } else {

                }
            }
        }
        int lastCount = FILTER.size();
        log.info("table 갯수 " + tables.size());
        simpleContext.checkCount(lastCount - count, docUrl, code);
        if (hasUnknown) {
            throw new Exception("UNKNOWN " + docUrl);
        }
    }
}
