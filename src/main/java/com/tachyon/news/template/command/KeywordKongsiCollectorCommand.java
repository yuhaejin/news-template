package com.tachyon.news.template.command;

import com.github.mustachejava.Mustache;
import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.Keyword;
import com.tachyon.news.template.model.User;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.StringWriter;
import java.util.*;

/**
 * 중요한 키워드를 가진 공시를 노티하기 위해 공시텍스트를 확인하고 해당 공시를 DB에 저장함.
 */
@Slf4j
@Component
public class KeywordKongsiCollectorCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;

    @Autowired
    private TemplateMapper templateMapper;

    @Autowired(required = false)
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private Mustache mustache;

    @PostConstruct
    private void init() {
    }


    @Override
    public void execute(Message message) throws Exception {

        String key = myContext.findInputValue(message);
        if (isSemiRealTimeData(message) == false) {
            log.info("준실시간데이터가 아님.. " + key);
            return;
        }

        log.info(key + "<<< ");
        DocNoIsuCd docNoIsuCd = findDocNoIsuCd(key);

        Map<String, Object> kongsiHodler = findKongsiHalder(myContext, message, templateMapper, docNoIsuCd.getDocNo(), docNoIsuCd.getIsuCd(), docNoIsuCd.getAcptNo());
        if (kongsiHodler == null) {
            log.info("기초공시정보가 없음. " + key);
            return;
        }

        String txtFilePath = findPath(myContext.getHtmlTargetPath(), docNoIsuCd.getIsuCd(), docNoIsuCd.getDocNo(), "txt");
        if (isEmpty(txtFilePath)) {
            log.info("텍스트파일 경로를 알 수 없음... " + key);
            return;
        }

        File f = new File(txtFilePath);
        if (f.exists() == false) {
            log.info("텍스트파일이 존재하지 않음.... " + txtFilePath);
            return;
        }

        String c = FileUtils.readFileToString(f, "UTF-8");
        c = BizUtils.removeBlank(c);

        // 여기에 이미 처리된 키워드를 보관함..
        Map<String, String> temp = new HashMap<>();
        Map<String, User> telegramMap = myContext.getTelegramMap();
        synchronized (telegramMap) {
            for (String userid : telegramMap.keySet()) {
                if ("admin".equalsIgnoreCase(userid)) {
                    continue;
                }
                User user = telegramMap.get(userid);
                // admin에 공통키워드가 존재함.
                User admin = telegramMap.get("admin");
                findKeyword(user, admin, kongsiHodler, key, docNoIsuCd.getIsuCd(), docNoIsuCd.getAcptNo(), c, temp);
            }
        }

        // 키워드 indexOf 확인하기.
        // 포함 키워드를 모두 체크하는게 좋겠지?
        // 있다면 DB에 저장하기.
        // 순번, datetime, keywords, 공시제목, 공시url, code, acptNo,전송여부. 생성시간, 업데이트시간. (NoticeHolder)

        log.info("done " + key);
    }

    private void findKeyword(User user, User admin, Map<String, Object> kongsiHodler, String key, String code, String acptNo, String c, Map<String, String> temp) {
        String keyword = findKeyword(c, user.getKeywords(), temp);
        if (isEmpty(keyword)) {
            // 공통 키워드 체크..
            keyword = findKeyword(c, admin.getKeywords(), temp);
            if (isEmpty(keyword)) {
                log.info(user.getUserid() + " 키워드가 존재하지 않음.... " + key);
                return;
            }
        }

        temp.put(keyword, key);
        sendNotification(user, keyword, kongsiHodler, key, code, acptNo);
    }


    private String findJsonPath(String acptNo) {
        String basePath = myContext.getJsonTargetPath();
        String day = acptNo.substring(0, 8);
        if (basePath.endsWith("/")) {
            basePath = basePath + day;
        } else {
            basePath = basePath + "/" + day;
        }

        Collection<File> files = listFiles(new File(basePath));
        for (File f : files) {
            String baseName = FilenameUtils.getBaseName(f.getAbsolutePath());
            if (baseName.contains(acptNo)) {
                return f.getAbsolutePath();
            }
        }

        return null;
    }

    private void sendNotification(User user, String keyword, Map<String, Object> kongsiHolder, String key, String code, String acptNo) {

        // 필요한 정보를 header로 전송하자..
        log.info(key + " " + keyword + " >>>");
        String docNm = Maps.getValue(kongsiHolder, "doc_nm");
        String docUlr = Maps.getValue(kongsiHolder, "doc_url");
        String rptNm = Maps.getValue(kongsiHolder, "rpt_nm");
        String acptUrl = getItemUrl(acptNo);
        String name = findCompanyName(code);

        String userId = user.getUserid();
        int chatId = user.getChatId();

        String _message = makeMessage(keyword, rptNm, acptUrl, name, docNm, docUlr);
        rabbitTemplate.convertAndSend("_TELEGRAM", (Object) key, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                message.getMessageProperties().getHeaders().put("chatId", chatId);
                message.getMessageProperties().getHeaders().put("userId", userId);
                message.getMessageProperties().getHeaders().put("message", _message);
                return message;
            }
        });


    }

    private String makeMessage(String keyword, String rptNm, String acptUrl, String name, String docNm, String docUlr) {
        StringWriter message = new StringWriter();
        mustache.execute(message, scopes(DateUtils.toString(new Date(), "yyyy.MM.dd HH:mm"), keyword, name, docUlr, docNm, acptUrl, rptNm));
//
//        message.append("봇이름: tachyonnews_bot").append("\n")
//                .append("발생시간: " + DateUtils.toString(new Date(), "yyyy.MM.dd HH:mm")).append("\n")
//                .append("키워드: " + keyword).append("\n")
//                .append("기업명: " + name).append("\n")
//                .append("공시명: " + docNm).append("\n")
//                .append(docUlr).append("\n")
//                .append("기초공시명: " + rptNm).append("\n")
//                .append(acptUrl);
        return message.toString();
    }


    private Map<String, Object> scopes(String time, String keyword, String company, String docUrl, String docNm, String acptUrl, String acptNm) {
        Map<String, Object> scopes = new HashMap<String, Object>();
        scopes.put("time", time);
        scopes.put("keyword", keyword);
        scopes.put("company", company);
        scopes.put("docUrl", docUrl);
        scopes.put("docNm", docNm);
        scopes.put("acptUrl", acptUrl);
        scopes.put("acptNm", acptNm);
        return scopes;
    }

    private String makeMessage() {
        String c = "<html>\n" +
                "<body>\n" +
                "봇이름: tachyonnews_bot <br />\n" +
                "발생시간: 2019.05.28 15:46<br />\n" +
                "키워드: <b>계약</b><br />\n" +
                "기업명: <b>한진칼</b><br />\n" +
                "공시명: <a href=\"http://kind.krx.co.kr/external/2019/05/28/000370/20190528000911/00636.htm\">주식등의대량보유상황보고서(일반) (2019.05.28)</a><br />\n" +
                "기초공시명: <a href=\"http://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno=20190528000370\">주식등의대량보유상황보고서(일반)</a><br />\n" +
                "</body>\n" +
                "</html>";

        return c;
    }


    private String findCompanyName(String code) {
        String name = myContext.findComName(code);
        if (isEmpty(name)) {
            name = templateMapper.findName(code);
            if (isEmpty(name) == false) {
                myContext.putCompany(code, name);
            }
        }


        return name;
    }

    public String getItemUrl(String acptNo) {
        return "http://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno=" + acptNo;
    }

    private String findKeyword(String c, List<Keyword> keywords, Map<String, String> temp) {
        if (c == null || keywords == null || keywords.size() == 0) {
            return null;
        }
        for (Keyword keyword : keywords) {
            if (temp.containsKey(keyword.getKeyword())) {
                return keyword.getKeyword();
            }
            int tmp = c.indexOf(keyword.getKeyword());
            if (tmp >= 0) {
                return keyword.getKeyword();
            }
        }

        return null;
    }

    private boolean isSemiRealTimeData(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__SEMI_REAL_TIME")) {
            if ("YES".equalsIgnoreCase((String) message.getMessageProperties().getHeaders().get("__SEMI_REAL_TIME"))) {
                return true;
            }
        }

        return false;
    }
}
