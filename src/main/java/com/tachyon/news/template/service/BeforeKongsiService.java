package com.tachyon.news.template.service;

import com.tachyon.news.template.command.Command;
import com.tachyon.news.template.command.CommandFactory;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;

/**
 * 로컬에서 뭔가 처리하기 전에 공시 관련 환경을 구성한다.
 * KEY > QUEUE  이런 형식이면 좋다.
 */
@Slf4j
@Service
public class BeforeKongsiService extends AbstractService {

    @Autowired
    private MyContext myContext;

    @Autowired
    private CommandFactory commandFactory;
    public BeforeKongsiService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    @Override
    public void consume(Message message) {

        if (myContext.isDev() == false) {
            return;
        }
        try {
            String inputValue = myContext.findInputValue(message);
            log.info(".. "+inputValue);
            String[] ss = StringUtils.splitByWholeSeparator(inputValue, ">");
            if (ss.length != 2) {
                log.info("INVAlID "+inputValue);
                return;
            }
            String queue = ss[1].trim();
            String key = ss[0].trim();

            Message keyMessage = makeMessage(key);

            // 로컬 공시 환경 구축
            NewsService kongsiCollector = commandFactory.findService(LocalKongsiService.class);
            kongsiCollector.consume(keyMessage);

            Command command = commandFactory.findCommand(queue);
            command.execute(keyMessage);

        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }

    }

    private Message makeMessage(String key) {
        return new Message(key.getBytes(Charset.forName("UTF-8")), new MessageProperties());
    }
}
