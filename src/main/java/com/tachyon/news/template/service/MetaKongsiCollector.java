package com.tachyon.news.template.service;

import com.tachyon.crawl.kind.KrxCrawler;
import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.crawl.kind.model.Kongsi;
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * 메타공시 수집..
 */
@Slf4j
@Service
public class MetaKongsiCollector extends AbstractService {
    @Autowired
    private MyContext myContext;

    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;
    @Autowired
    private RabbitTemplate rabbitTemplate;

    public MetaKongsiCollector(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    public void consume(Message message) {
        try {
            infoView(message, log);
            String key = myContext.findInputValue(message);
            log.info("<<< " + key);
            DocNoIsuCd docNoIsuCd = findDocNoIsuCd(key);
            String docNo = docNoIsuCd.getDocNo();
            String code = docNoIsuCd.getIsuCd();
            String acptNo = docNoIsuCd.getAcptNo();

            Map<String, Object> kongsiMap = findKongsiMap(myContext, message, docNo, code, acptNo);
            if (kongsiMap == null) {
                log.error("기초공시가 없음. " + key);
                return;
            }

            String day = acptNo.substring(0, 8);
            String parentJsonPath = findParentMetaJsonPath(myContext.getJsonTargetPath(), day);
            log.info("parentJsonPath "+parentJsonPath);
            File parentDir = new File(parentJsonPath);

            if (parentDir.exists() == false) {
                parentDir.mkdirs();
            }
            boolean doCrawl = false;
            File metaFile = findMetaJsonFileName(parentDir,acptNo);
            if (metaFile != null) {
                String metaJson = FileUtils.readFileToString(metaFile, "UTF-8");
                if (isEmpty(metaJson.trim())) {
                    doCrawl = false;
                } else {
                    Kongsi _kongsi = JsonUtils.toObject(metaJson, Kongsi.class);
                    if (_kongsi.getUrlInfos().size() == 0) {
                        //메타공시 수집...
                        doCrawl = true;
                        log.info("메타공시는 있으나 수집된 상태는 아님.. "+metaJson);

                    } else {
                        log.info("메타공시파일정상 " + metaFile.getAbsolutePath()+" "+key);
                        doCrawl = false;
                    }
                }

            } else {
                log.info("메타공시파일이 없음 . "+key);
                doCrawl = true;
            }


            if (doCrawl == true) {
                // 기초공시파일 존재하는지 확인
                String name = findName(code);
                Kongsi kongsi = from(kongsiMap, name);
                String jsonPath = findMetaJsonPath(kongsi, myContext.getJsonTargetPath(), day, code);
                log.info("metaJsonPath "+jsonPath);
                new KrxCrawler().addUrlInfo2(kongsi, loadBalancerCommandHelper, acptNo);
                //메타공시 수집..
                String c = JsonUtils.toJson(kongsi);
                FileUtils.write(new File(jsonPath), c, "UTF-8");
            }


            log.info("done "+key);


        } catch (Exception e) {
            handleError(rabbitTemplate, message, e, log);
        }
    }


    private File findMetaJsonFileName(File dir,String acptNo) {

        Collection<File> fileCollectoin = listFiles(dir);
        for (File f : fileCollectoin) {
            String baseName = FilenameUtils.getBaseName(f.getAbsolutePath());
            if (baseName.contains(acptNo)) {
                return f;
            }

        }


        return null;
    }

    private Kongsi from(Map<String, Object> kongsiMap, String name) {
        Kongsi kongsi = new Kongsi();
        kongsi.setTns_dt(Maps.getValue(kongsiMap, "tns_dt"));
        kongsi.setRpt_nm(Maps.getValue(kongsiMap, "rpt_nm"));
        kongsi.setSubmit_oblg_nm(Maps.getValue(kongsiMap, "submit_nm"));
        kongsi.setAcpt_no(Maps.getValue(kongsiMap, "acpt_no"));
        kongsi.setItem_name(name);
        return kongsi;
    }

    private String findParentMetaJsonPath(String jsonPath, String day) {
        if (jsonPath.endsWith("/")) {
            return jsonPath + day;
        } else {
            return jsonPath + "/" + day;
        }
    }

    private String findMetaJsonPath(Kongsi kongsi, String jsonPath, String day, String code) {
        String name = kongsi.getItem_name();
        if (isEmpty(name)) {
            name = "SPACE";
        }
        if (isEmpty(code)) {
            code = "SPACE";
        }
        String _name = kongsi.getSubmit_oblg_nm();

        return jsonPath + "/" + day + "/" + kongsi.getAcpt_no() + "_" + code + "_" + name + "_" + _name + ".json";
    }


}
