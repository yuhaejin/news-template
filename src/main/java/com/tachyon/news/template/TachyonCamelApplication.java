package com.tachyon.news.template;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.tachyon.news.template.repository")
public class TachyonCamelApplication {
    public static void main(String[] args) {
        SpringApplication.run(TachyonCamelApplication.class, args);
    }
}
