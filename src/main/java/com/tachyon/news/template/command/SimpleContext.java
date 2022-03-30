package com.tachyon.news.template.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;

@Slf4j
public class SimpleContext {
    private int count;
    private double ratio;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getRatio() {
        return ratio;
    }

    public void setRatio(double ratio) {
        this.ratio = ratio;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("count", count)
                .toString();
    }

    public void checkCount(int count,String docUrl,String code) {
        double d = (double)count / getCount();
        ratio = d * 100;
        log.info("CHECKCOUNT "+getCount()+" "+count+" "+(  d * 100 )+"% "+code+" "+docUrl);
    }
}
