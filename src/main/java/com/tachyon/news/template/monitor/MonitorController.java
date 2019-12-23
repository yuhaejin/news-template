package com.tachyon.news.template.monitor;

import com.tachyon.crawl.kind.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class MonitorController {

    @RequestMapping(value = "/monitor", method = RequestMethod.GET)
    @ResponseStatus(value = HttpStatus.OK)
    public String isRunning() {
        long ration = Utils.findMemoryRatio();
        if (ration <= 5) {
            return "FAIL_"+ration;
        } else {
            return "OK";

        }
    }
}
