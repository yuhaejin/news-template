package com.tachyon.news.template.model;

import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.Maps;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 * 속보 키워드를 가진 공시 정보 가짐..
 */
public class TelegramHolder implements Serializable {
    private String docNo;
    private String keyword;
    private String isuCd;
    private String acptNo;
    private String docNm;
    private String docUrl;
    private String acptNm;
    private String tndDt;

    public String getTndDt() {
        return tndDt;
    }

    public void setTndDt(String tndDt) {
        this.tndDt = tndDt;
    }

    public String getDocNo() {
        return docNo;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getIsuCd() {
        return isuCd;
    }

    public void setIsuCd(String isuCd) {
        this.isuCd = isuCd;
    }

    public String getAcptNo() {
        return acptNo;
    }

    public void setAcptNo(String acptNo) {
        this.acptNo = acptNo;
    }

    public String getDocNm() {
        return docNm;
    }

    public void setDocNm(String docNm) {
        this.docNm = docNm;
    }

    public String getDocUrl() {
        return docUrl;
    }

    public void setDocUrl(String docUrl) {
        this.docUrl = docUrl;
    }

    public String getAcptNm() {
        return acptNm;
    }

    public void setAcptNm(String acptNm) {
        this.acptNm = acptNm;
    }

    public static TelegramHolder from(Map<String, Object> map) {
        TelegramHolder telegramHolder = new TelegramHolder();
        String acptNo = getValue(map, "acpt_no");
        telegramHolder.setDocNo(getValue(map, "doc_no"));
        telegramHolder.setIsuCd(getValue(map, "isu_cd"));
        telegramHolder.setAcptNo(acptNo);
        telegramHolder.setKeyword(getValue(map, "keyword"));
        telegramHolder.setAcptNm(getValue(map, "rpt_nm"));
        telegramHolder.setDocUrl(getValue(map, "doc_url"));
        telegramHolder.setDocNm(getValue(map, "doc_nm"));
        String tnsDt = getValue(map, "tns_dt");
        if (tnsDt.length() == 4) {
            tnsDt = acptNo.substring(0, 8) +tnsDt;
        }
        tnsDt = convert(tnsDt);
        telegramHolder.setTndDt(tnsDt);
        return telegramHolder;
    }

    private static String convert(String tnsDt) {
        try {
            //199901260000
            Date date = DateUtils.toDate(tnsDt, "yyyyMMddHHmm");
            return DateUtils.toString(date, "yyyy.MM.dd HH:mm");
        } catch (Exception e) {
            return DateUtils.toString(new Date(), "yyyy.MM.dd HH:mm");
        }
    }

    private static String getValue(Map<String, Object> map,String keyword) {
        return Maps.getValue(map, keyword);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("docNo", docNo)
                .append("keyword", keyword)
                .append("isuCd", isuCd)
                .append("acptNo", acptNo)
                .append("docNm", docNm)
                .append("docUrl", docUrl)
                .append("acptNm", acptNm)
                .toString();
    }
}
