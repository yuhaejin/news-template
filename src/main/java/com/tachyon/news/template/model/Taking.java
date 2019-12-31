package com.tachyon.news.template.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class Taking implements Serializable {
    private String name;
    private String birth;
    private String taking;              //자기자금
    private String borrowing;           // 차입급
    private String etc;
    private String sum;
    private String resource;            //취득자금 조성경위 및 원천
    private String borrowingAmount;     //차입금액
    private String borrower;            //차입처
    private String borrowingPeriod;     //차입기간
    private String collateral;          // 담보내역

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

    /**
     * 차입금 테이블의 차임금액
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
                .append("borrowingAmount", borrowingAmount)
                .append("borrower", borrower)
                .append("borrowingPeriod", borrowingPeriod)
                .append("collateral", collateral)
                .toString();
    }
}
