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

    int findKrRssPubDateCount(String rssPubDate);

    void insertKrRssPubDate(String rssPubDate);
    void insertStaff(Map<String, Object> param);
    int findStaff(Map<String, Object> param);

    void updateStockHolderBirthDay(Map<String, Object> map);
    /**
     * 친인척 table 에 처음으로 등장하는지 체크한다.
     * code, name, birth으로 조회함.
     * @param param
     * @return
     */
    int findRelativeHodlerSize(Map<String, Object> param);

    int findDupStockCountOnSameKind(Map<String, Object> param);
    List<Long> findDupStockSeqOnSameKind(Map<String, Object> param);

    int findDupStockCountOnOtherKind(Map<String, Object> param);
    List<Long> findDupStockSeqOnOtherKind(Map<String, Object> param);

    /**
     * 정정 임원ㆍ주요주주특정증권등소유상황보고서 공시인 경우 이전 공시 데이터 삭제함.
     * @param code
     * @param docNo
     */
    void deleteBeforeStaffHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int findStaffCount(@Param("isuCd")String code, @Param("name")String name, @Param("birth")String birth);

    int findLastDayStaffCount(@Param("isuCd")String code, @Param("name")String name, @Param("birth_day")String birth, @Param("kongsi_day")String endDate);

    String findCode(String codeNm);

    void deleteBeforePerfHolder(@Param("doc_no")String docNo, @Param("isu_cd")String code);

    int findDuplicatePerf(Map<String, Object> param);

    void insertPerf(Map<String, Object> param);

    void insertDupStock(Map<String, Object> param);

    int findDupStockCount(Map<String, Object> param);
}
