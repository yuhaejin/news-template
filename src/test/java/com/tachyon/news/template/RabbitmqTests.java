package com.tachyon.news.template;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitmqTests {
    @Autowired
    private AmqpAdmin amqpAdmin;

    @Test
    public void purgeQueue() throws Exception {
        Integer count = (Integer) amqpAdmin.getQueueProperties("T_STOCKHOLDER").get("QUEUE_MESSAGE_COUNT");
        System.out.println(count);
    }

}
