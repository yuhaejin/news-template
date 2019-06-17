package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        String docUrl = (String)kongsiHodler.get("doc_url");
        // 속보 키워드 목록 가져옴..
        // TODO 사용자별로 키워드를 가지면 아래 로직 변경해야 함.
        // (현재는 모두 동일하므로 아래처럼 하나의 키워드가 존재할 수 있지만 사용자마다 키워드가 다르면 모든 키워드를 대상으로 확인해 봐야 함.)
        List<String> telegramKeywordList = myContext.getTelegramKeywordList();
        // 여기에 이미 처리된 키워드를 보관함..
        List<String> keywords = new ArrayList<>();
        synchronized (telegramKeywordList) {
            String keyword = findKeyword(c, telegramKeywordList);
            if (isEmpty(keyword)) {
                log.info("속보 키워드가 없음.  " + key + " " + docUrl);
            } else {
                log.info("속보키워드 <<< "+keyword+" "+key+" "+docUrl);
                keywords.add(keyword);
            }
        }
        // 사용자별 키워드가 없으므로 아래는 최대 하나 키워드가 존재..
        for (String keyword : keywords) {
            templateMapper.insertTelegramHolder(docNoIsuCd.getDocNo(), docNoIsuCd.getIsuCd(), docNoIsuCd.getAcptNo(), keyword);
        }


        log.info("done " + key);
    }

    private String findKeyword(String c, List<String> telegramList) {
        for (String keyword : telegramList) {
            if (c.indexOf(keyword) >= 0) {
                return keyword;
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
