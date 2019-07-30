package com.tachyon.news.template.service;

import com.tachyon.news.template.BaseObject;
import com.tachyon.news.template.NewsConstants;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractService extends BaseObject implements NewsService {
    private TemplateMapper templateMapper;

    public AbstractService(TemplateMapper templateMapper) {
        this.templateMapper = templateMapper;
    }

    protected Map<String,Object> findKongsiHalder2(String docNo,String code,String acptNo) {
        return templateMapper.findKongsiHalder2(docNo, code, acptNo);
    }

    protected File findJsonBasicKongsiPath(String jsonTargetPath, String acptNo, String code) {
        String dir = findJsonFilePath(jsonTargetPath, acptNo.substring(0, 8));
        File ff = new File(dir);
        if (ff.exists() == false) {
            return null;
        }
        Collection<File> files = listFiles(ff);
        for (File f : files) {
            String baseName = FilenameUtils.getBaseName(f.getAbsolutePath());
            if (baseName.startsWith(acptNo + "_" + code)) {
                return f;
            }
        }
        return null;
    }
    private File findMetaKongsi(String jsonTargetPath, String code, String acptNo) {
        return findJsonBasicKongsiPath(jsonTargetPath, acptNo,code);

    }

    protected File findHtmlKongsiPath(String htmlTargetPath,String docNo, String code) {
        return new File(fileHtmlPath(htmlTargetPath, docNo, code));
    }

    public String fileHtmlPath(String htmlTargetPath, String docNo, String isuCd) {
        return this.findPath(htmlTargetPath, isuCd, docNo, "htm");
    }

    protected String findPath(String htmlTargetPath, String code, String docNo, String ext) {
        String year = docNo.substring(0, 4);
        return htmlTargetPath + "/" + code + "/" + year + "/" + docNo + "_" + code + "." + ext;
    }


    private Collection<File> listFiles(String dir) {
        return listFiles(new File(dir));
    }
    protected String findJsonFilePath(String jsonTargetPath,String day) {
        String dir = null;
        if (jsonTargetPath.endsWith("/")) {
            dir = jsonTargetPath +  day;
        } else {
            dir = jsonTargetPath + "/" + day;
        }

        return dir;
    }

    protected void requestBodyAndHeaders2(RabbitTemplate rabbitTemplate, String uri, Object body,Map<String,Object> headers) {

        rabbitTemplate.convertAndSend(uri, body,convert(headers));
    }

    private MessagePostProcessor convert(Map<String, Object> headers) {
        return new MessagePostProcessor(){
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                for (String key : headers.keySet()) {
                    message.getMessageProperties().getHeaders().put(key, headers.get(key));
                }
                return message;
            }
        };
    }
    protected String findKey(Map<String, Object> kongsi) {
        StringBuilder builder = new StringBuilder();
        int index =0;
        for (String key : kongsi.keySet()) {
            if (index == 0) {
                builder.append(key);
            } else {
                builder.append(",").append(key);
            }

            index++;
        }

        return builder.toString();
    }

    protected void setupKongsiMap(Map<String, Object> headers, Map<String, Object> kongsi) {
        headers.putAll(kongsi);
        String key = findKey(kongsi);
        headers.put(NewsConstants.KONGSI_KEY, key);
    }

    /**
     * 이전 Process에서 받은 헤더정보를 셋업한다.
     * KongsiCollector에서 설정한 헤더정보를 이후에도 사용할 수 있게 함.
     * @param message
     * @param headers
     */
    protected void setupPreviousHeaders(Message message, Map<String, Object> headers) {
        Map<String, Object> in = message.getMessageProperties().getHeaders();
        for (String key : in.keySet()) {
            if (key.startsWith("__")) {
                headers.put(key, in.get(key));
            }
        }
    }

    private void toErrorQueue(RabbitTemplate rabbitTemplate,String body,Map<String, Object> headers) {
        requestBodyAndHeaders2(rabbitTemplate,NewsConstants.ERROR_QUEUE,body,headers);
    }

    protected void handleError(RabbitTemplate rabbitTemplate, Message message, Exception e, Logger logger) {
        String key = findKey(message);
        String fromQueue = findRoutingKey(message);
        Map<String, Object> headers = makeHeaders(fromQueue,e);
        toErrorQueue(rabbitTemplate,key,headers);
        logger.error(key+" "+e.getMessage(),e);
    }

    private Map<String, Object> makeHeaders(String fromQueue,Exception e) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(NewsConstants.FROM_QUEUE, fromQueue);
        headers.put(NewsConstants.ERR_MSG, StringUtils.abbreviate(e.toString(),200));

        return headers;
    }
    protected String findKey(Message message) {
        byte[] bytes= message.getBody();
        return new String(bytes, Charset.forName("UTF-8"));
    }

    protected String findRoutingKey(Message message) {
        return message.getMessageProperties().getReceivedRoutingKey();
    }


    protected void saveError(String fromQueue, String value,String error) {

        templateMapper.saveError(fromQueue, value, error);
    }



    protected Map<String, Object> findKongsiMap(MyContext myContext, Message message, String docNo, String code, String acptNo) {

        Map<String, Object> kongsiMap = myContext.findKongsiMap(message);
        if (kongsiMap == null) {
            kongsiMap = findKongsiHalder2(docNo, code, acptNo);
        }

        return kongsiMap;
    }


    protected boolean isEmpty(String value) {
        return StringUtils.isEmpty(value);
    }

    protected String findName(String code) {
        return templateMapper.findName(code);
    }
}
