package com.tachyon.news.template.model;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.Serializable;

import static org.apache.commons.lang3.StringUtils.isEmpty;

@Slf4j
public class Borrowing implements Serializable {
    private String borrowingAmount;     //차입금액
    private String borrower;            //차입처
    private String borrowingPeriod;     //차입기간
    private String collateral;          // 담보내역

    private String errorMsg;
    private String unit;

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public static Logger getLog() {
        return log;
    }

    /**
     * 차입금 테이블의 차임금액
     *
     * @param borrowingAmount
     */
    public void setBorrowingAmount(String borrowingAmount) {
        this.borrowingAmount = borrowingAmount;
    }

    public String getBorrowingAmount() {
        return borrowingAmount;
    }

    /**
     * 차입자
     *
     * @param borrower
     */
    public void setBorrower(String borrower) {

        this.borrower = borrower;
    }

    public String getBorrower() {
        return borrower;
    }

    /**
     * 차임기간
     *
     * @param borrowingPeriod
     */
    public void setBorrowingPeriod(String borrowingPeriod) {
        this.borrowingPeriod = borrowingPeriod;
    }

    public String getBorrowingPeriod() {
        return borrowingPeriod;
    }

    public void setCollateral(String collateral) {
        this.collateral = collateral;
    }

    public String getCollateral() {
        return collateral;
    }


    public String validate(String name, String key) {

        if (isEmpty(borrower)) {
            log.warn("차입금인데 차입처가 없음. " + name + " < " + key);
            return "차입금인데 차입처가 없음";
        }
        if (isEmpty(borrowingAmount)) {
            log.warn("차입금인데 차입금액정보가 없음. " + name + " < " + key);
            return "차입금인데 차입금액정보가 없음. ";
        }
        if (isEmpty(borrowingPeriod)) {
            log.warn("차입금인데 차입기간 없음. " + name + " < " + key);
            return "차입금인데 차입기간 없음. ";
        }
        if (isEmpty(collateral)) {
            log.warn("차입금인데 담보내역없음. " + name + " < " + key);
            return "차입금인데 담보내역없음. ";
        }
        return "OK";
    }


}
