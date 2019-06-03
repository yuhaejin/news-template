package com.tachyon.news.template.jobs;

import com.tachyon.news.template.config.MyContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableScheduling
public class NewsJobs {
    @Autowired
    private MyContext myContext;

    @Scheduled(cron = "0 0 9-20 ? * MON-FRI")
    public void setupTelegramInfo() {
        myContext.setupTelegramMap();
    }
}
