package com.tachyon.news.template.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;

import java.io.File;
import java.io.IOException;
import java.util.List;
@Slf4j
public class FileMonitorListener extends FileAlterationListenerAdaptor {
    private MyContext myContext;

    public FileMonitorListener(MyContext myContext) {
        this.myContext = myContext;
    }

    @Override
    public void onFileChange(File file) {
        log.info("onFileChange "+file.getAbsolutePath());
        String path = file.getAbsolutePath();
        String name = FilenameUtils.getName(path);
        if (name.equalsIgnoreCase("investor.txt")==false) {
            log.info("SKIP "+file.getAbsolutePath());
            return;
        }

        try {
            log.info("initializeInvestor ... "+file.getAbsolutePath());
            List<String> lines = FileUtils.readLines(file, "UTF-8");
            myContext.initializeInvestor(lines);
        } catch (IOException e) {

        }
    }
}
