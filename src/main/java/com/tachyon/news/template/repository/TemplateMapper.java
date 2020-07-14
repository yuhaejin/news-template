package com.tachyon.news.template.repository;

import com.tachyon.crawl.kind.model.Staff;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Mapper
public interface TemplateMapper {
    Map<String, Object> findKongsiHalder2(@Param("doc_no") String docNo, @Param("isu_cd") String isuCd, @Param("acpt_no") String acptNo);

    long insertStockHolder(Map<String, Object> paramStockHolder);

    int findStockHolder(Map<String, Object> param);

    List<Map<String, Object>> findBeforeKongsi(@Param("isuCd")String code, @Param("acptNo")String acptNo);

    void deleteBeforeStockHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);


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
    void insertTelegramHolder(Map<String, Object> param);

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
    int findTelegramHolder(Map<String, Object> param);

    void deleteOverlapRumor(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo);

    List<Map<String, Object>> findChangeWithTelegram(@Param("doc_no")String docNo, @Param("isu_cd")String isuCd, @Param("acpt_no")String acptNo);


    int findRssPubDateCount(String rssPubDate);

    void insertRssPubDate(String rssPubDate);

    void insertLargestStockHolder(Map<String, Object> param);

    void deleteBeforeLargestStockHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int findLargestStockHolderCount(Map<String, Object> keyParam);

    void deleteBeforeRelativeHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

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

    void deleteBeforePerfHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int findDuplicatePerf(Map<String, Object> param);

    void insertPerf(Map<String, Object> param);

    void insertDupStock(Map<String, Object> param);

    int findDupStockCount(Map<String, Object> param);

    void insertTrotHolder(Map<String, Object> kongsiHodler);

    void insertKongsiHolder(Map<String, Object> result);

    int findKongsiCount(@Param("doc_no")String docNo, @Param("isu_cd")String code, @Param("acpt_no")String acptNo);

    int findTakingCount(@Param("doc_no")String docNo, @Param("isu_cd")String code, @Param("acpt_no")String acptNo);

    void deleteBeforeTakingHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int fincTakingHolderCount(@Param("isu_cd")String code, @Param("name")String name, @Param("birth")String birth, @Param("owner_capital")String taking, @Param("borrow_amount")String borrowing, @Param("etc")String etc);
    int fincTakingHolderCount(Map<String, Object> param);

    void insertTakingHolder(Map<String, Object> param);

    int findGiveNtake(Map<String, Object> param);

    void insertGiveNTake(Map<String, Object> param);

    void deleteBeforeMyStockHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int findMyStockCount(@Param("doc_no")String docNo, @Param("isu_cd")String code, @Param("acpt_no")String acptNo);
    int findMyStockCount(Map<String, Object> param);

    void insertMyStock(Map<String, Object> param);

    int findSupplyContractCount(@Param("contract_gubun")String contractGubun, @Param("isu_cd")String code, @Param("action_type")String actionYype,@Param("do_date")String doDate);
    int findSupplyContractCount(Map<String, Object> param);

    void deleteBeforeContractHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    void insertSupplyContract(Map<String, Object> param);

    Map<String, Object> findNewerStockHolder(@Param("isu_cd")String isuCd, @Param("owner_name")String name, @Param("change_date")Timestamp timestamp);

    void deleteBeforeTouchHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int findTouchHolderCount(@Param("doc_no")String docNo, @Param("isu_cd")String code, @Param("acpt_no")String acptNo);
    int findTouchHolderCount(Map<String, Object> param);

    void insertTouchHolder(Map<String, Object> param);

    /**
     * code 종목, name 투자자, beforeDate 보다 크고 nowDate보다 미만인 경우를 찾음..
     * @param code
     * @param name
     * @param beforeDate
     * @param nowDate
     * @return
     */
    int findBeforeStock(@Param("isu_cd")String code,@Param("owner_name")String name, @Param("before_date") String beforeDate, @Param("now_date") String nowDate);

    void compressStockHolder(Long seq);

    void deleteBeforeTrialHolder(@Param("isu_cd")String code, @Param("doc_no")String docNo);

    int findTrialCount(@Param("doc_no")String docNo, @Param("isu_cd")String code, @Param("acpt_no")String acptNo);
    int findTrialCount(Map<String, Object> map);

    void insertTrial(Map<String, Object> map);

    Integer findCloseByStringDate(@Param("code")String code, @Param("date")Timestamp date);

    List<Staff> findStaffHolder(@Param("doc_no")String docNo, @Param("isu_cd")String code, @Param("acpt_no")String acptNo);

    void updateSimpleStockHolderBirthDay(@Param("seq")long seq, @Param("birth_day")String birthDay,@Param("birth_ym")String birthYm);

    int countBirth(@Param("kongsi_birth")String kongsiBirth, @Param("acpt_year")String acptYear);

    void insertBirth(@Param("kongsi_birth")String kongsiBirth, @Param("parsed_birth")String parsedBirth, @Param("open_birth")String birth, @Param("nm")String name, @Param("doc_no")String docNo, @Param("isu_cd")String code, @Param("acpt_no")String acptNo, @Param("doc_url")String docUrl, @Param("acpt_year")String acptYear);
}
