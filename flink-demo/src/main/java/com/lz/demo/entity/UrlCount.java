package com.lz.demo.entity;

import java.sql.Timestamp;

public class UrlCount {
    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public UrlCount() {
    }

    public UrlCount(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
