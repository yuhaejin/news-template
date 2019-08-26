package com.tachyon.news.template.model;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.util.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class LargestStock implements Serializable {
    private long seq;
    private String name;
    private String birth;
    private String relation;
    private String isuCd;
    private String changeDate;
    private String gender;
    private String homeAbroad;
    private String doubleup1;
    private String doubleup2;
    private long generalCnt;
    private double generalRatio;
    private long kindCnt;
    private double kindRatio;
    private long stockDepositoryCnt;
    private double stockDepositoryRation;
    private long firstCnt;
    private double firstRatio;
    private String nationality;

    private String docNo;
    private String acptNo;

    public String getNationality() {
        return nationality;
    }

    public void setNationality(String nationality) {
        this.nationality = nationality;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBirth() {
        return birth;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public String getIsuCd() {
        return isuCd;
    }

    public void setIsuCd(String isuCd) {
        this.isuCd = isuCd;
    }

    public String getChangeDate() {
        return changeDate;
    }

    public void setChangeDate(String changeDate) {
        this.changeDate = changeDate;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getHomeAbroad() {
        return homeAbroad;
    }

    public void setHomeAbroad(String homeAbroad) {
        this.homeAbroad = homeAbroad;
    }

    public String getDoubleup1() {
        return doubleup1;
    }

    public void setDoubleup1(String doubleup1) {
        this.doubleup1 = doubleup1;
    }

    public String getDoubleup2() {
        return doubleup2;
    }

    public void setDoubleup2(String doubleup2) {
        this.doubleup2 = doubleup2;
    }

    public long getGeneralCnt() {
        return generalCnt;
    }

    public void setGeneralCnt(long generalCnt) {
        this.generalCnt = generalCnt;
    }

    public double getGeneralRatio() {
        return generalRatio;
    }

    public void setGeneralRatio(double generalRatio) {
        this.generalRatio = generalRatio;
    }

    public long getKindCnt() {
        return kindCnt;
    }

    public void setKindCnt(long kindCnt) {
        this.kindCnt = kindCnt;
    }

    public double getKindRatio() {
        return kindRatio;
    }

    public void setKindRatio(double kindRatio) {
        this.kindRatio = kindRatio;
    }

    public long getStockDepositoryCnt() {
        return stockDepositoryCnt;
    }

    public void setStockDepositoryCnt(long stockDepositoryCnt) {
        this.stockDepositoryCnt = stockDepositoryCnt;
    }

    public double getStockDepositoryRation() {
        return stockDepositoryRation;
    }

    public void setStockDepositoryRation(double stockDepositoryRation) {
        this.stockDepositoryRation = stockDepositoryRation;
    }

    public long getFirstCnt() {
        return firstCnt;
    }

    public void setFirstCnt(long firstCnt) {
        this.firstCnt = firstCnt;
    }

    public double getFirstRatio() {
        return firstRatio;
    }

    public void setFirstRatio(double firstRatio) {
        this.firstRatio = firstRatio;
    }

    public String getDocNo() {
        return docNo;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public String getAcptNo() {
        return acptNo;
    }

    public void setAcptNo(String acptNo) {
        this.acptNo = acptNo;
    }

    public static LargestStock from(Map<String, Object> map,String code,String docNo,String acptNo) {
        if (map == null) {
            return null;
        }
        LargestStock largestStock = new LargestStock();
        largestStock.setName(find(map,"성명"));
        String birth = find(map, "생년월일", "등록번호");       // OR 로 찾음..

        largestStock.setBirth(findBirth(birth));
        largestStock.setGender(find(map,"성별"));
        largestStock.setHomeAbroad(find(map,"국내외구분"));
        largestStock.setNationality(find(map,"국적"));
        largestStock.setRelation(find(map,"관계"));
        largestStock.setDoubleup1(find(map,"겸직내용1"));
        largestStock.setDoubleup2(find(map,"겸직내용2"));
        largestStock.setGeneralCnt(findLongWith2(map,"보통","주식수"));  // AND로 찾음.
        largestStock.setGeneralRatio(findDoubleWith2(map,"보통","비율"));   // AND로 찾음.
        largestStock.setKindCnt(findLongWith(map,"종류주식주식수"));
        largestStock.setKindRatio(findDoubleWith(map,"종류주식비율"));
        largestStock.setStockDepositoryCnt(findLongWith(map,"증권예탁증권주식수"));
        largestStock.setStockDepositoryRation(findDoubleWith(map,"증권예탁증권비율"));
        largestStock.setFirstCnt(findLongWith2(map,"우선","주식수"));    // AND로 찾음. 변동키워드가 있으면 변동후 AND 추가
        largestStock.setFirstRatio(findDoubleWith2(map,"우선","비율")); // AND로 찾음. 변동키워드가 있으면 변동후 AND 추가
        String changeDate = find(map, "변동일");
        if (isEmpty(changeDate) || "-".equalsIgnoreCase(changeDate)) {
            largestStock.setChangeDate(docNo.substring(0,8));
        } else {
            largestStock.setChangeDate(findBirth(changeDate));
        }
        largestStock.setIsuCd(code);
        largestStock.setDocNo(docNo);
        largestStock.setAcptNo(acptNo);




        return largestStock;
    }

    private static boolean isEmpty(String value) {
        return StringUtils.isEmpty(value);
    }

    private static String findBirth(String birth) {
        StringBuilder sb = new StringBuilder();
        char[] chars = birth.toCharArray();
        for (char c : chars) {
            if (Character.isDigit(c)) {
                sb.append(c);
            } else if (c == '-') {
                sb.append(c);
            } else if (c == '*') {
                sb.append(c);
            } else {

            }
        }

        return sb.toString();
    }


    private static String find(Map<String, Object> map, String... keys) {
        for (String _key : map.keySet()) {
            if (StringUtils.containsAny(_key,keys)) {
                return Maps.getValue(map, _key);
            }
        }

        return "";
    }
    private static String find2(Map<String, Object> map, String... keys) {
        for (String _key : map.keySet()) {
            if (BizUtils.containsAll(_key,keys)) {
                return Maps.getValue(map, _key);
            }
        }

        return "";
    }

    private static long findLongWith(Map<String, Object> map, String... keys) {
        for (String _key : map.keySet()) {
            if (BizUtils.containsAll(_key,keys)) {
                String value = Maps.getValue(map, _key);
                if ("-".equalsIgnoreCase(value) || "".equalsIgnoreCase(value)) {
                    return 0;
                } else {
                    return Maps.getLongValue(map, _key);
                }
            }
        }

        return 0;
    }
    private static long findLongWith2(Map<String, Object> map, String... keys) {
        for (String _key : map.keySet()) {
            if (_key.contains("변동")) {
                if (_key.contains("변동후")) {
                    if (BizUtils.containsAll(_key, keys)) {
                        String value = Maps.getValue(map, _key);
                        if ("-".equalsIgnoreCase(value) || "".equalsIgnoreCase(value)) {
                            return 0;
                        } else {
                            return Maps.getLongValue(map, _key);
                        }
                    }
                } else {
                    continue;
                }
            }else {
                if (BizUtils.containsAll(_key,keys)) {
                    String value = Maps.getValue(map, _key);
                    if ("-".equalsIgnoreCase(value) || "".equalsIgnoreCase(value)) {
                        return 0;
                    } else {
                        return Maps.getLongValue(map, _key);
                    }
                }
            }
        }

        return 0;
    }
    private static double findDoubleWith(Map<String, Object> map, String... keys) {
        for (String _key : map.keySet()) {
            if (BizUtils.containsAll(_key,keys)) {
                String value = Maps.getValue(map, _key);
                if ("-".equalsIgnoreCase(value) || "".equalsIgnoreCase(value)) {
                    return 0;
                } else {
                    return Maps.getDoubleValue(map, _key);
                }
            }
        }

        return 0;
    }
    private static double findDoubleWith2(Map<String, Object> map, String... keys) {
        for (String _key : map.keySet()) {
            if (_key.contains("변동")) {
                if (_key.contains("변동후")) {
                    if (BizUtils.containsAll(_key,keys)) {
                        String value = Maps.getValue(map, _key);
                        if ("-".equalsIgnoreCase(value) || "".equalsIgnoreCase(value)) {
                            return 0;
                        } else {
                            return Maps.getDoubleValue(map, _key);
                        }
                    }
                }
            }else {
                if (BizUtils.containsAll(_key,keys)) {
                    String value = Maps.getValue(map, _key);
                    if ("-".equalsIgnoreCase(value) || "".equalsIgnoreCase(value)) {
                        return 0;
                    } else {
                        return Maps.getDoubleValue(map, _key);
                    }
                }
            }
        }

        return 0;
    }
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("seq", seq)
                .append("name", name)
                .append("birth", birth)
                .append("relation", relation)
                .append("isuCd", isuCd)
                .append("changeDate", changeDate)
                .append("gender", gender)
                .append("homeAbroad", homeAbroad)
                .append("doubleup1", doubleup1)
                .append("doubleup2", doubleup2)
                .append("generalCnt", generalCnt)
                .append("generalRatio", generalRatio)
                .append("kindCnt", kindCnt)
                .append("kindRatio", kindRatio)
                .append("stockDepositoryCnt", stockDepositoryCnt)
                .append("stockDepositoryRation", stockDepositoryRation)
                .append("firstCnt", firstCnt)
                .append("firstRatio", firstRatio)
                .append("nationality", nationality)
                .append("docNo", docNo)
                .append("acptNo", acptNo)
                .toString();
    }

    public Map<String, Object> paramMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name",name);
        map.put("birthday",birth );
        map.put("isu_cd",isuCd );
        map.put("changedate",changeDate );
        map.put("gender",gender );
        map.put("homeabroad",homeAbroad );
        map.put("relation", relation);
        map.put("doubleup1",doubleup1 );
        map.put("doubleup2",doubleup2 );
        map.put("generalstockcnt",generalCnt );
        map.put("generalratio", generalRatio);
        map.put("kindstockcnt", kindCnt);
        map.put("kindratio", kindRatio);
        map.put("stockdepositorycnt", stockDepositoryCnt);
        map.put("stockdepositoryratio",stockDepositoryRation );
        map.put("firststockratio",firstRatio );
        map.put("firststockcnt",firstCnt );
        map.put("doc_no",docNo );
        map.put("acpt_no",acptNo );
        map.put("nationality",nationality );
        return map;
    }

    public Map<String, Object> keyParam() {
        Map<String, Object> map = new HashMap<>();
        map.put("name",name);
        map.put("birthday",birth );
        map.put("isu_cd",isuCd );
        map.put("changedate",changeDate );
        return map;
    }
}
