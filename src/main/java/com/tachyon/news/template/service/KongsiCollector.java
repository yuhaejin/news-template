package com.tachyon.news.template.service;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.KrxCrawler;
import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.crawl.kind.model.Kongsi;
import com.tachyon.crawl.kind.util.JSoupHelper;
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.NewsConstants;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 공시 파일과 텍스트 파일 처리
 */
@Slf4j
@Service
public class KongsiCollector extends AbstractService {
    @Autowired
    private MyContext myContext;

    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RetryTemplate retryTemplate;

    public KongsiCollector(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    private Map<String, String> temp = new HashMap<>();
    public void consume(Message message) {
        try {
            infoView(message,log);
            String key = myContext.findInputValue(message);
            log.info("<<< " + key);

            String path = myContext.getSkipDocNoIsuCdAcptNoFilePath();
            File f = new File(path);
            if (f.exists() == false) {

            }else {
                List<String> strings = FileUtils.readLines(f, "UTF-8");
                if (strings.size() > 0) {
                    for (String line : strings) {
                        temp.put(line, line);
                    }
                }
            }

            if (temp.containsKey(key)) {
                log.error("SkIP " + key);
                return;
            }
            DocNoIsuCd docNoIsuCd = findDocNoIsuCd(key);
            String docNo = docNoIsuCd.getDocNo();
            String code = docNoIsuCd.getIsuCd();
            String acptNo = docNoIsuCd.getAcptNo();

            // 기초공시 정보 수집
            Map<String, Object> kongsiMap = findKongsiMap(myContext, message, docNo, code, acptNo);
            if (kongsiMap == null) {
                log.error("기초공시가 없음. "+key);
                return;
            }
            String docUrl = Maps.getValue(kongsiMap, "doc_url");
            // 공시 원본 파일 체크 및 처리.
            String filePath = BizUtils.htmlFilePath(myContext.getHtmlTargetPath(), docNo, code, "htm");
            File htmlFile = new File(filePath);
            String content = null;
            if (htmlFile.exists()) {
                if (BizUtils.isValid(htmlFile)) {
                    //유효한 파일이라면 그대로 읽기
                    content = FileUtils.readFileToString(htmlFile, "UTF-8");
                    log.info("공시 정상파일 존재.. "+htmlFile+" fileSize="+content.length());
                } else {
                    // 파일 수집과 저장.
                    content = findBody(loadBalancerCommandHelper,retryTemplate, docUrl);
                    log.info("공시 비정상파일 존재.. "+htmlFile+" fileSize="+content.length());
                    saveFile(filePath, content);
                }
            } else {
                // 파일 수집과 저장.
                content = findBody(loadBalancerCommandHelper,retryTemplate, docUrl);
                log.info("공시파일 미존재.. "+htmlFile+" fileSize="+content.length());
                saveFile(filePath, content);
            }

            // 텍스트 파일 체크 및 처리..
            if (validate(content) == false) {
                log.error(" INVALID HTML "+key+" "+filePath+" "+docUrl);
                return;
            }
            String text = convertText(content);
            log.info("텍스트파일 처리... "+htmlFile+" textSize="+text.length());
            String txtPath = findPath(myContext.getHtmlTargetPath(), code, docNo, "txt");
            File txtFile = new File(txtPath);
            boolean reindexing = false;
            if (txtFile.exists()) {
                if (txtFile.length() != text.getBytes("UTF-8").length) {
                    // 텍스트파일이 크기가 다르다면..
                    log.info("파일존재하지만 크기가 달라 재처리.. "+txtPath);
                    saveFile(txtPath, text);
                    // 현재 20191227에는 Spc 관련 작업하지 않으므로 일단 막아둠.
//                    if (hasSpcLpKeyword(text)) {
//                        String spcLpPath = findSpcLpPath(myContext.getSpcLpPath(), code, docNo, acptNo, "txt");
//                        saveFile(spcLpPath, text);
//                    }
                    reindexing = true;
                } else {
                    reindexing = false;
                }
            } else {
                // 파일이 없다면..
                log.info("파일 미존재.. "+txtPath);
                saveFile(txtPath, text);
                // 현재 20191227에는 Spc 관련 작업하지 않으므로 일단 막아둠.
//                if (hasSpcLpKeyword(text)) {
//                    String spcLpPath = findSpcLpPath(myContext.getSpcLpPath(), code, docNo, acptNo, "txt");
//                    saveFile(spcLpPath, text);
//                }
                reindexing = true;
            }
            log.info("공시파일 처리 완료..."+key);
            if (isLocalCollectKongsi(message)) {
                // 로컬공시 수집때에는 이후 처리하지 않음.
            } else {
                noticeKongsiCorrectComplete(key, kongsiMap,reindexing,message);
            }

        } catch (Exception e) {
            handleError(rabbitTemplate, message, e, log);
        }
    }

    private boolean hasSpcLpKeyword(String c) {
        if (c.contains(myContext.getSpcLpKeyword())) {
            return true;
        } else {
            return false;
        }
    }
    private boolean isLocalCollectKongsi(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__LOCAL_KONGSI")) {
            return true;
        } else {
            return false;
        }
    }
    private String convertText(String content) {
        return BizUtils.extractText(content);
    }
    private boolean validate(String raw) {
        raw = raw.trim();
        if (raw.endsWith("</HTML>")) {
            return true;
        } else if (raw.endsWith("</html>")) {
            return true;
        } else {
            return false;
        }
    }

    private String findBody(LoadBalancerCommandHelper loadBalancerCommandHelper,RetryTemplate retryTemplate, String docUrl) throws IOException {
        log.info("공시파일 수집.. "+docUrl);
        if (loadBalancerCommandHelper == null) {
            return JSoupHelper.findBody(docUrl,retryTemplate);
        } else {
            return loadBalancerCommandHelper.findBody(docUrl,retryTemplate);
        }
    }
    private void saveFile(String filePath, String content) throws IOException {
        log.info("파일저장.. "+filePath);
        FileUtils.write(new File(filePath), content, "UTF-8", false);
    }


    private String tnsDt(String tnsDt, String day) {
        if (tnsDt.length() == 4) {
            if (day.length() >= 8) {
                tnsDt = day.substring(0, 8) + tnsDt;
            } else {
                // 이런 경우는 없음..
                tnsDt = day + tnsDt;
            }
        }

        return tnsDt;
    }

    private void noticeKongsiCorrectComplete(String key, Map<String, Object> kongsiMap,boolean reindexing,Message message) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(NewsConstants.KONGSI_CHECK, NewsConstants.KONGSI_CHECK);
        if (reindexing) {
            headers.put(NewsConstants.REINDEXING, NewsConstants.REINDEXING);
        }

        setupPreviousHeaders(message,headers);
        setupKongsiMap(headers,kongsiMap);
        requestBodyAndHeaders2(rabbitTemplate,"OK_GATEWAY", key,headers );
//        requestBodyAndHeaders(camelContext,"rabbitmq:OK_GATEWAY?queue=OK_GATEWAY&routingKey=OK_GATEWAY&autoDelete=false", key,headers );

    }

    private Kongsi from(Map<String, Object> kongsiMap) {
        Kongsi kongsi = new Kongsi();
        kongsi.setRpt_nm(Maps.getValue(kongsiMap,"rpt_nm"));
        kongsi.setTns_dt(Maps.getValue(kongsiMap,"tns_dt"));
        kongsi.setAcpt_no(Maps.getValue(kongsiMap,"acpt_no"));

        return kongsi;
    }

    private void collectMetaKongsi(String docNo, String code, String acptNo, Map<String, Object> kongsiMap) throws Exception {
        log.debug("... collectMetaKongsi code="+code+" acptNo="+acptNo);
        String day = acptNo.substring(0, 8);
        KrxCrawler krxCrawler = new KrxCrawler();
        Kongsi kongsi = from(kongsiMap);
        krxCrawler.addUrlInfo(kongsi, loadBalancerCommandHelper, acptNo);
        String lastJson = JsonUtils.toJson(kongsi);
        String jsonPath = findMetaKongsiPath(kongsi, code, day);

        FileUtils.write(new File(jsonPath),lastJson,"UTF-8",false);
    }

    private String findMetaKongsiPath(Kongsi kongsi,String code,String day) {
        String name = kongsi.getItem_name();
        if (StringUtils.isEmpty(name)) {
            name = "SPACE";
        }
        if (StringUtils.isEmpty(code)) {
            code = "SPACE";
        }
        String _name = kongsi.getSubmit_oblg_nm();
        return myContext.getJsonTargetPath() + "/" + day + "/" + kongsi.getAcpt_no() + "_" +code+"_"+ name+"_" +_name+ ".json";
    }

}
