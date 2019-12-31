package com.tachyon.news.template.service;

import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * 로컬 환경에 기초공시 정보 셋팅함. (수동으로 하기에 너무 리소스가 많이 들어가서 만들어봄)
 * 1. 공시키로 기초공시 정보 gcp에서 가져와 로컬DB에 셋팅함.
 * 2. 공시파일 KongsiCollector 를 이용하여 수집함.
 *
 */
@Slf4j
@Service
public class LocalKongsiService extends AbstractService {
    @Autowired
    private MyContext myContext;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private Environment environment;

    public LocalKongsiService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    public void consume(Message message) {
        try {
            infoView(message,log);
            String key = myContext.findInputValue(message);
            log.info("<<< " + key);

            if ("prod".equalsIgnoreCase(findCurrentProfile())) {
                log.info("SKIP 운영환경에서는 실행하지 않음...");
                return;
            }

            DocNoIsuCd docNoIsuCd = findDocNoIsuCd(key);
            String docNo = docNoIsuCd.getDocNo();
            String code = docNoIsuCd.getIsuCd();
            String acptNo = docNoIsuCd.getAcptNo();


            RestTemplate restTemplate = new RestTemplate();
            Map<String,Object> result = restTemplate.getForObject(findKongsiApi(docNo,code,acptNo), Map.class);
            log.info("<<< "+result);
            if (templateMapper.findKongsiCount(docNo, code, acptNo) == 0) {
                templateMapper.insertKongsiHolder(result);
            }
            Map<String, Object> headers = new HashMap<>();
            headers.put("__LOCAL_KONGSI", "YES");
            requestBodyAndHeaders2(rabbitTemplate,"KONGSI_CHECK", key,headers );

            log.info(key+" >>> KONGSI_CHECK");
        } catch (Exception e) {
            handleError(rabbitTemplate, message, e, log);
        }
    }

    private String findCurrentProfile() {
        String[] strings = environment.getActiveProfiles();
        if (strings.length == 0) {
            return "prod";
        }

        return strings[0];
    }
    private String findKongsiApi(String docNo, String code, String acptNo) {
        return "http://localhost:8080/api/v1/kongsi?docNo=" + docNo + "&isuCd=" + code + "&acptNo=" + acptNo;
    }


    /**
     * //http://localhost:8080/api/v1/kongsi?docNo=20141106000679&isuCd=A-&acptNo=20181116000232
     * @param key
     * @return
     */
    private String findKongsiApi(String key) {
        DocNoIsuCd docNoIsuCd = findDocNoIsuCd(key);
        String docNo = docNoIsuCd.getDocNo();
        String code = docNoIsuCd.getIsuCd();
        String acptNo = docNoIsuCd.getAcptNo();

        return findKongsiApi(docNo, code, acptNo);
    }

}
