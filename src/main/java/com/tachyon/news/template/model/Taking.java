package com.tachyon.news.template.model;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Taking implements Serializable {
    private String name;
    private String birth;
    private String taking;              //자기자금
    private String borrowing;           // 차입급
    private String etc;
    private String sum;
    private String resource;            //취득자금 조성경위 및 원천
//    private String borrowingAmount;     //차입금액
//    private String borrower;            //차입처
//    private String borrowingPeriod;     //차입기간
//    private String collateral;          // 담보내역

    private String type;                // 자기자금(MY),차입금(BORROWING)

    public static String MY = "MY";
    public static String BORROWING = "BORROWING";


    private String[] generals = new String[3];
    private String errorMsg;
    private String spot;

    private List<Borrowing> borrowings;
    private String unit;

    public void addBorrowings(Borrowing borrowing) {
        if (borrowings == null) {
            borrowings = new ArrayList<>();
        }
        borrowings.add(borrowing);
    }

    public List<Borrowing> getBorrowings() {
        return borrowings;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String findKey() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public String getBirth() {
        return birth;
    }

    public void setTaking(String taking) {
        this.taking = taking;
    }

    public String getTaking() {
        return taking;
    }

    public void setBorrowing(String borrowing) {
        this.borrowing = borrowing;
    }

    public String getBorrowing() {
        return borrowing;
    }

    public void setEtc(String etc) {
        this.etc = etc;
    }

    public String getEtc() {
        return etc;
    }

    public void setSum(String sum) {
        this.sum = sum;
    }

    public String getSum() {
        return sum;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getResource() {
        return resource;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("birth", birth)
                .append("taking", taking)
                .append("borrowing", borrowing)
                .append("etc", etc)
                .append("sum", sum)
                .append("resource", resource)
                .append("type", type)
                .append("generals", generals)
                .append("errorMsg", errorMsg)
                .append("spot", spot)
                .append("borrowings", borrowings)
                .append("unit", unit)
                .toString();
    }

    /**
     * 빈값이거나 0 이면..
     *
     * @param s
     * @return
     */
    private boolean isEmpty(String s) {
        if ("0".equalsIgnoreCase(s)) {
            return true;
        } else {
            return StringUtils.isEmpty(s);
        }
    }

    public String validate(String key) {

        if ("Y".equalsIgnoreCase(generals[0])) {
            return "자기자금 상세내역이없음. ";
        }
        if ("Y".equalsIgnoreCase(generals[1])) {
            return "차입금 상세내역이없음. ";
        }
        if ("Y".equalsIgnoreCase(generals[2])) {
            return "기타 상세내역이없음. ";
        }
        if ("OK".equalsIgnoreCase(generals[0])) {
            String result = checkMy(key);
            if ("OK".equalsIgnoreCase(result) == false) {
                return result;
            }
        }
        if ("OK".equalsIgnoreCase(generals[1])) {
            String result = checkBorrow(key);
            if ("OK".equalsIgnoreCase(result) == false) {
                return result;
            }
        }
        if ("OK".equalsIgnoreCase(generals[2])) {
            String result = checkMy(key);
            if ("OK".equalsIgnoreCase(result) == false) {
                return result;
            }
        }

        if ("개요".equalsIgnoreCase(type)) {
            // 자기자본, 차입금이 없을 때..
            String result = checkGeneral(key);
            if ("OK".equalsIgnoreCase(result) == false) {
                return result;
            }
        }

        return "OK";

    }


    private String checkGeneral(String key) {
        if (isEmpty(sum) == false) {
            log.warn("개요인데 계의 값이 존재함.. " + name + " < " + key);
            // 개요인데.. 계의 값이 있을 때...
            return "개요인데 계의 값이 존재함.";
        } else {
            return "OK";
        }

    }

    private String checkMy(String key) {
        if (isEmpty(resource)) {
            return "자기자금인데 조성경위가 없음.";
        } else {
            return "OK";
        }
    }

    private String checkBorrow(String key) {
        for (Borrowing borrowing : borrowings) {
            String s = borrowing.validate(name, key);
            if ("OK".equalsIgnoreCase(s)==false) {
                return s;
            }
        }

        return "OK";
    }

    public void setupGeneral() {
        generals[0] = "N";
        generals[1] = "N";
        generals[2] = "N";
        if (isEmpty(taking) == false) {
            generals[0] = "Y";
        }
        if (isEmpty(borrowing) == false) {
            generals[1] = "Y";
        }
        if (isEmpty(etc) == false) {
            generals[2] = "Y";
        }
    }

    public void setupGneral(String type) {
        if (MY.equalsIgnoreCase(type)) {
            String s = generals[0];
            if ("N".equalsIgnoreCase(s)) {
                generals[2] = "OK";
            }else {
                if ("OK".equalsIgnoreCase(s)) {
                    generals[2] = "OK";
                }else {
                    generals[0] = "OK";
                }
            }

        } else if (BORROWING.equalsIgnoreCase(type)) {
            generals[1] = "OK";
        }
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setSpot(String spot) {
        this.spot = spot;
    }

    public String getSpot() {
        return spot;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getUnit() {
        return unit;
    }

    public void setupUnit() {

    }
}
