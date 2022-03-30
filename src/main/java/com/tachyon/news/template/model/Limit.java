package com.tachyon.news.template.model;

import java.io.Serializable;

public class Limit implements Serializable {
    private int count;
    private long total;

    public Limit(int count, long total) {
        this.count = count;
        this.total = total;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long doExecute(long time) throws InterruptedException {
        count++;
        total += time;
        if (count >= 29) {
            long _l = total - 1000;
            if (_l <= 0) {
                //OK
                reset();
            } else {
                Thread.sleep(_l);
                reset();
                return _l;
            }
        } else {
            //TODO 추후 튜닝이 필요함.
        }

        return 0;
    }

    public void plusTime(long time) {
        total += time;
    }
    private void reset() {
        count = 0;
        total = 0;
    }
}
