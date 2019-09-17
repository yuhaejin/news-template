package com.tachyon.news.template.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Mapper
public interface TemplateMapper {
    Map<String, Object> findKongsiHalder2(@Param("doc_no") String docNo, @Param("isu_cd") String isuCd, @Param("acpt_no") String acptNo);

    void insertStockHolder(Map<String, Object> paramStockHolder);


    int findStockHolder(Map<String, Object> param);


    List<Map<String, Object>> findBeforeKongsi(@Param("isuCd")String code, @Param("acptNo")String acptNo);

    void deleteBeforeStockHolder(@Param("isuCd")String code, @Param("docNo")String docNo);


    String findName(String name);

    void insertStaffHolder(Map<String, Object> map);

    void insertAccessHolder(Map<String, Object> map);


    List<Map<String, Object>> findSimpleStaffHolder(Map<String, Object> map);

    void deleteBeforeStaffHolder(Map<String, Object> deleteParam);


    int findPurposeHolder(Map<String, Object> map);
    void insertPurposeHolder1(Map<String, Object> map);
    void insertPurposeHolder2(Map<String, Object> map);

    void saveError(@Param("from_queue")String fromQueue, @Param("body")String value, @Param("error")String error);


    List<Map<String, Object>> findKeywords();

    int findUser(String user);

    void updateChatId(@Param("userid")String user, @Param("chat_id")long chatid);

    void insertRumor(Map<String, Object> toParamMap);

    int findRumorCount(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo);

    void insertTelegramHolder(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo, @Param("keyword")String keyword);

    List<Map<String, Object>> findUsers();
    List<Map<String, Object>> findNoGroupTelegramHolder();
    List<Map<String, Object>> findGroupTelegramHolder();

    void completeNoGroupTelegramHolder(@Param("doc_no")String docNo, @Param("acpt_no")String acptNo, @Param("keyword")String keyword);

    List<Map<String, Object>> findMemberCode();

    void completeGroupTelegramHolder(@Param("doc_no")String docNo, @Param("acpt_no")String acptNo, @Param("keyword")String keyword);

    List<Map<String, Object>> findBots();

    Integer findClose(@Param("code")String code,@Param("date")Timestamp date);

    void insertExpiration(Map<String, Object> param);

    int findExpirationCount(Map<String, Object> param);

    int findTelegramHolder(@Param("doc_no")String docNo, @Param("acpt_no")String acptNo, @Param("keyword")String keyword);

    void deleteOverlapRumor(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo);

    List<Map<String, Object>> findChangeWithTelegram(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo);


    int findRssPubDateCount(String rssPubDate);

    void insertRssPubDate(String rssPubDate);

    void insertLargestStockHolder(Map<String, Object> param);

    void deleteBeforeLargestStockHolder(@Param("isuCd")String code, @Param("docNo")String docNo);

    int findLargestStockHolderCount(Map<String, Object> keyParam);

    void deleteBeforeRelativeHolder(@Param("isuCd")String code, @Param("docNo")String docNo);

    void insertRelativeHolder(Map<String, Object> param);

    int findRelativeHolderCount(Map<String, Object> param);

    List<Map<String, Object>> findRelativeWithTelegram(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo);

    List<Map<String, Object>> findRelativeCount(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo, @Param("name")String name, @Param("birth")String birth);
}
