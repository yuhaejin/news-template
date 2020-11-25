package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.model.Result;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.parser.TableParser;
import com.tachyon.crawl.kind.parser.WholeBodyParser;
import com.tachyon.crawl.kind.parser.handler.WholeSelector;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * 잠정적인 영업실적
 * 일단 영업 실적이 있고 전망은 없는 공시제목을 찾는다.
 * 마지막 테이블을 대상으로 처리한다.
 * 영업실적과 단위는 분리하여 저장한다.
 *
 */
@Slf4j
@Component
public class ProvisionalSalesPerformanceCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;

    @Autowired
    private TemplateMapper templateMapper;
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Override
    public void execute(Message message) throws Exception {
        String key = myContext.findInputValue(message);
        log.info("<<< "+key);

        TableParser tableParser = new TableParser(new WholeSelector());
        WholeBodyParser parser = new WholeBodyParser();

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
        String docUrl = Maps.getValue(kongsiHolder, "doc_url");
        if (docNm.contains("영업") && docNm.contains("실적")) {
            if (docNm.contains("전망") == false) {
                execute(key, code, docNo, acptNo, kongsiHolder, tableParser, parser);
            } else {
                log.info("SKIP 영업실적공시가 아님.. " + docNm + " docNo=" + docNo + " code=" + code + " acptNo=" + acptNo + " " + docUrl);
            }
        } else {
            log.info("SKIP 영업실적공시가 아님.. " + docNm + " docNo=" + docNo + " code=" + code + " acptNo=" + acptNo + " " + docUrl);
        }

        log.info("done . "+key);
    }

    private void execute(String key,String code,String docNo,String acptNo,Map<String, Object> map, TableParser tableParser, WholeBodyParser myParser) throws Exception {
        String docUrl = Maps.getValue(map, "doc_url");
        String docNm = Maps.getValue(map, "doc_nm");
        String path = findPath(myContext.getHtmlTargetPath(), code, docNo, "htm");
        log.info("... " + path + " " + key + " " + docUrl);
        File f = new File(path);
        if (f.exists() == false) {
            log.error("NO FILE " + path + " " + key);
            return;
        }
        String c = FileUtils.readFileToString(f, "UTF-8");
        List<Table> tables = tableParser.parseSome(c, docUrl, myParser);
        if (tables == null || tables.size() == 0) {
            log.error("NO TABLES " + docUrl + " " + key);
            return;
        }
        Table table = tables.get(0);
        List<List<String>> lists = table.getBodies();
        Result result = new Result();
        result.parse(lists);

        int validResult = result.validate();
        if (validResult != Result.VALID) {
            log.error("INVALID " +toString(validResult)+" "+ docUrl + " " + key);
            return;
        }
        if (result.isAllValueEmpty()) {
            // 이런 경우는 일반적인 실적공시와 형태가 달라 처리하기 SKIP함.
            log.error("ALL VALUE EMPTY " + docUrl + " " + key);
            return;
        }

        // 정정일 때는 이전 데이터 삭제한다.
        if (isChangeKongsi(docNm)) {
            String _docNo = findBeforeKongsi(templateMapper,code, acptNo);
            log.info("이전PerfHolder 확인 code=" + code + " acpt_no=" + acptNo + " docNo=" + _docNo);
            if (StringUtils.isEmpty(_docNo) == false) {
                if (docNo.equalsIgnoreCase(_docNo) == false) {
                    deleteBeforePerfHolder(templateMapper, code, _docNo);
                    log.info("이전PerfHolder 삭제 code=" + code + " docNo=" + _docNo);
                }
            }
        }


// 키로 같은 데이터가 있는지 확인 없으면 INSERT 있으면 SKIP
        String quarter = result.parseQuarter2(acptNo);
        String type = findType(docNm);
        Map<String,Object> param = result.param(docNo,code,acptNo);
        param.put("type", type);
        param.put("child_com", table.getChildCompany());
        modify(param,quarter,result.getQuarter());
        if (isDuplicate(templateMapper, param) == false) {
            log.info("INSERT " + key + " " + docUrl +" "+param);
            insertPerf(templateMapper, param);
            String seq = findPk(param);
            sendToArticleQueue(rabbitTemplate, seq, "BIZPERFORMANCE", seq);
        } else {
            log.info("SKIP 중복됨 "+ key + " " + docUrl+" "+param);
        }
    }

    /**
     * 재무제표 유형 확인
     * 연결재무제표, 별도재무제표
     * @param docNm
     * @return
     */
    private String findType(String docNm) {
        if (docNm.contains("연결재무제표")) {
            return "CFS";   //연결재무제표
        } else {
            return "SFS";   //별도재무제표
        }
    }

    private String toString(int valid) {
        if (Result.NO_TITLE == valid) {
            return "NO_TITLE";
        } else if (Result.NO_VALUE == valid) {
            return "NO_VALUE";
        } else if (Result.NO_INFO == valid) {
            return "NO_INFO";
        } else if (Result.NO_UNIT == valid) {
            return "NO_UNIT";
        } else if (Result.INVALID_VALUE == valid) {
            return "INVALID_VALUE";
        } else {
            return "VALID";
        }
    }

    private void modify(Map<String, Object> param ,String quarter,String rawQuarter) {
        modifyLength(param, "provider_date");
        modifyLength(param, "provider_positon");
        modifyLength(param, "provider");
        modifyLength(param, "subject");
        modifyLength(param, "contact");
        param.put("raw_quarter", rawQuarter);
        modifyQuarter(param,quarter);
    }

    private void modifyQuarter(Map<String, Object> param, String quarter) {
        if (quarter.startsWith("_")==false) {
            String[] strings = StringUtils.splitByWholeSeparator(quarter, "/");
            if (strings.length == 2) {
                param.put("year", strings[0]);
                param.put("quarter_tp", strings[1]);
            }
        }
        param.put("quarter", quarter);
    }

    private void modifyLength(Map<String, Object> param, String key) {
        String s = Maps.getValue(param, key);
        s = StringUtils.abbreviate(s, 50);
        param.put(key, s);
    }

    private void insertPerf(TemplateMapper templateMapper, Map<String, Object> param) {
        templateMapper.insertPerf(param);
    }

    private boolean isDuplicate(TemplateMapper templateMapper,Map<String, Object> param) {
        if (templateMapper.findDuplicatePerf(param) > 0) {
            return true;
        } else {
            return false;
        }
    }

    private void deleteBeforePerfHolder(TemplateMapper templateMapper, String code, String docNo) {
        templateMapper.deleteBeforePerfHolder(code, docNo);
    }



}
