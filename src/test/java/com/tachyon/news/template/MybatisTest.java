package com.tachyon.news.template;

import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@Slf4j
public class MybatisTest {
    @Autowired
    private TemplateMapper templateMapper;

    @Test
    public void findStockData() throws ParseException {
        long price = 7000;
        int percentage = findPercentage(price);
        if (percentage >= 30) {
            log.info("ODD " + percentage + "%");
        } else {
            log.info("NOODD " + percentage + "%");
        }
    }
    @Test
    public void findStockData2() throws ParseException {
        long price = 8000;
        int percentage = findPercentage(price);
        if (percentage >= 30) {
            log.info("ODD " + percentage + "%");
        } else {
            log.info("NOODD " + percentage + "%");
        }
    }
    private int findPercentage(long price) throws ParseException {
        int closePrice  = templateMapper.findCloseByStringDate("A002710", convert("2000-01-06 00:00:00"));
        long value = Math.abs(price - closePrice);
        int percentage = percentage(value, closePrice);
        log.info("ODD "+value + "/ " + closePrice + "  =>  " + percentage + "%");
        return percentage;
    }

    private int percentage(double value, double closePrice) {
        return (int)(value * 100 / closePrice);
    }
    private Timestamp convert(String date) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date _date = simpleDateFormat.parse(date);
        return new Timestamp(_date.getTime());
    }
}
