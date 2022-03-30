package com.tachyon.news.template.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class DocNoIsuCd {
    private String docNo;
    private String isuCd;
    private String acptNo;

    public DocNoIsuCd(String docNo, String isuCd, String acptNo) {
        this.docNo = docNo;
        this.isuCd = isuCd;
        this.acptNo = acptNo;
    }


    public String getAcptNo() {
        return acptNo;
    }

    public String getDocNo() {
        return docNo;
    }

    public String getIsuCd() {
        return isuCd;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("docNo", docNo)
                .append("isuCd", isuCd)
                .append("acptNo", acptNo)
                .toString();
    }
}
