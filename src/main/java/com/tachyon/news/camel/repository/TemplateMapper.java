package com.tachyon.news.camel.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

@Mapper
public interface TemplateMapper {
    Map<String, Object> findKongsiHalder2(@Param("doc_no") String docNo, @Param("isu_cd") String isuCd, @Param("acpt_no") String acptNo);
    int findStockChangeHistory(String docNo);
    Map<String, Object> findKongsiHalder(String docNo);
    List<Map<String, Object>> findKongsiHalder3(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd);

    void insertStockHolder(Map<String, Object> paramStockHolder);

    void insertStockChangeHistory(Map<String, Object> param);

    int findStockHolder(Map<String, Object> param);

    void updateStockHolder(Map<String, Object> paramStockHolder);

    int findKongsiHolder(@Param("isuCd") String isuCd,@Param("docNo")String docNo,@Param("acptNo")String acptNo);

    void insertKongsiHoder(Map<String, Object> map);

    void updateKongsiHoder(Map<String, Object> map);

    List<Map<String, Object>> findBeforeKongsi(@Param("isuCd")String code, @Param("acptNo")String acptNo);

    void deleteBeforeStockHolder(@Param("isuCd")String code, @Param("docNo")String docNo);

    List<Map<String, Object>> findStockHolderPrice(Map<String, Object> param);

    String findCode(String name);
    String findName(String name);
    List<Map<String,Object>> findFullCode();
    int findStockData(Map<String, Object> map);
    void insertStockData(Map<String, Object> map);
    List<String> findKongsiHalders(String code);

    String findCode2(String name);

    void insertIsuHolder(@Param("isuName")String name, @Param("isuCd")String isuCd);
    void insertStaffHolder(Map<String, Object> map);
    void updateStaffHolder(Map<String, Object> map);
    int findStaffHolderCount(Map<String, Object> map);

    void insertAccessHolder(Map<String, Object> map);

    void updateKongsiHoderCode(Map<String, Object> map);

    List<Map<String, Object>> findSimpleStaffHolder(Map<String, Object> map);

    void deleteBeforeStaffHolder(Map<String, Object> deleteParam);

    int findDbBasicKongsiCount(String date);


    int findDailyBasicKongsi(String date);

    void updateDailyBasicKongsi(Map<String, Object> map);
    void insertDailyBasicKongsi(Map<String, Object> map);


    List<Map<String, Object>> findKongsiHalderByAcptNo(@Param("acpt_dt")String acptDt, @Param("acpt_no")String acptNo);
    List<Map<String, Object>> findKongsiHalderByDocNo(String docNo);

    public void insertCompany(Map<String, Object> company);

    String findCode3(@Param("acpt_dt")String acptDt, @Param("doc_no")String docNo);
    int findStockHolderCountByCodeNo(@Param("isu_cd")String code, @Param("doc_no")String docNo);
    int findStaffHolderCountByCodeNo(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int findPurposeHolder(Map<String, Object> map);
    void insertPurposeHolder1(Map<String, Object> map);
    void insertPurposeHolder2(Map<String, Object> map);

    void saveError(@Param("from_queue")String fromQueue, @Param("body")String value, @Param("error")String error);
}
