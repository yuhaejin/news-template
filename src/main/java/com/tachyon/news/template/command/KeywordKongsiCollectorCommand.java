package com.tachyon.news.template.command;

import com.google.common.collect.TreeBasedTable;
import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.crawl.kind.util.JSoupHelper;
import com.tachyon.crawl.kind.util.JSoupTableException;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.jobs.InfixToPostfixParens;
import com.tachyon.news.template.jobs.StackNode;
import com.tachyon.news.template.model.CorrectBean;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;

import static com.tachyon.crawl.BizUtils.findBodies;
import static com.tachyon.crawl.BizUtils.findTitles;

/**
 * 중요한 키워드를 가진 공시를 노티하기 위해 공시텍스트를 확인하고 해당 공시를 DB에 저장함.
 */
@Slf4j
@Component
public class KeywordKongsiCollectorCommand extends BasicCommand {
    @Autowired
    private MyContext myContext;

    @Autowired
    private TemplateMapper templateMapper;
    private static final String RESULT_PRIFIX = "R:::";
    private InfixToPostfixParens converter = new InfixToPostfixParens();

    @PostConstruct
    private void init() {
    }


    @Override
    public void execute(Message message) throws Exception {

        String key = myContext.findInputValue(message);
        if (isSemiRealTimeData(message) == false) {
            log.info("준실시간데이터가 아님.. " + key);
            return;
        }

        log.info(key + "<<< ");
        DocNoIsuCd docNoIsuCd = findDocNoIsuCd(key);

        Map<String, Object> kongsiHodler = findKongsiHalder(myContext, message, templateMapper, docNoIsuCd.getDocNo(), docNoIsuCd.getIsuCd(), docNoIsuCd.getAcptNo());
        if (kongsiHodler == null) {
            log.info("기초공시정보가 없음. " + key);
            return;
        }

        String docUrl = Maps.getValue(kongsiHodler, "doc_url");
        // 정정공시중에 원공시 정보는 SKIP
        if (isOldAtCorrectedKongsi(kongsiHodler)) {
            log.info("SKIP 정정공시중의 이전 공시임. " + key + " " + docUrl);
            return;
        }
        log.info(key+" "+docUrl);
//        String c = null;
        String contents = "";
        StringBuilder sb = new StringBuilder();

        if (isCorrectedKongsi(kongsiHodler)) {
            //정정공시인 경우에는 변경된 데이터에서 처리한다.
            String htmlFilePath = findPath(myContext.getHtmlTargetPath(), docNoIsuCd.getIsuCd(), docNoIsuCd.getDocNo(), "htm");
            if (isEmpty(htmlFilePath)) {
                log.info("Html파일 경로를 알 수 없음... " + key);
                return;
            }

            File f = new File(htmlFilePath);
            if (f.exists() == false) {
                log.info("Html파일이 존재하지 않음.... " + htmlFilePath + " " + key);
                return;
            }

            String c = FileUtils.readFileToString(f, "UTF-8");
            int index = StringUtils.indexOfAny(c, "COVER-TITLE", "SECTION-1", "xforms_title");
            if (index >= 0) {
                c = c.substring(0, index);
            }

            // 1. 테이블에서 정정후 데이터찾기
            findCorrectTable(c, sb, key);

            // 2. 정정사항 테이블 이후 데이터 중에서 정정후 데이터만 찾기..
            findEtcCorrect(c, sb, key, index);

        } else {
            String txtFilePath = findPath(myContext.getHtmlTargetPath(), docNoIsuCd.getIsuCd(), docNoIsuCd.getDocNo(), "txt");
            if (isEmpty(txtFilePath)) {
                log.info("텍스트파일 경로를 알 수 없음... " + key);
                return;
            }

            File f = new File(txtFilePath);
            if (f.exists() == false) {
                log.info("텍스트파일이 존재하지 않음.... " + txtFilePath + " " + key);
                return;
            }

            String c = FileUtils.readFileToString(f, "UTF-8");
            sb.append(c);
        }

        contents = BizUtils.removeBlank(sb.toString());

        // 속보 키워드 목록 가져옴..
        // TODO 사용자별로 키워드를 가지면 아래 로직 변경해야 함.
        // (현재는 모두 동일하므로 아래처럼 하나의 키워드가 존재할 수 있지만 사용자마다 키워드가 다르면 모든 키워드를 대상으로 확인해 봐야 함.)
        List<String> telegramKeywordList = myContext.getTelegramKeywordList();
        // 여기에 이미 처리된 키워드를 보관함..
        List<String> keywords = new ArrayList<>();
        synchronized (telegramKeywordList) {
            String keyword = findKeyword(contents, telegramKeywordList);
            if (isEmpty(keyword)) {
                log.info("속보 키워드가 없음.  " + key + " " + docUrl);
            } else {
                log.info("속보키워드 <<< " + keyword + " " + key + " " + docUrl);
                keywords.add(keyword);
            }
        }
        // 사용자별 키워드가 없으므로 아래는 최대 하나 키워드가 존재..
        for (String keyword : keywords) {
            int count = templateMapper.findTelegramHolder(docNoIsuCd.getDocNo(), docNoIsuCd.getAcptNo(), keyword);
            if (count == 0) {
                templateMapper.insertTelegramHolder(docNoIsuCd.getDocNo(), docNoIsuCd.getIsuCd(), docNoIsuCd.getAcptNo(), keyword);
            }
        }


        log.info("done " + key);
    }

    /**
     * 정정사항 테이블 이외 데이터에서 정정후 데이터만 추출
     *
     * @param c
     * @param sb
     * @param corectIndex
     */
    private void findEtcCorrect(String c, StringBuilder sb, String key, int corectIndex) {
        if (corectIndex != -1) {
            c = c.substring(corectIndex);
        }

        List<CorrectBean> beans = findCorrectInfo(c);
        log.debug("CorrectBean size "+beans.size());
        for (int i = 0; i < beans.size(); i++) {
            CorrectBean bean = beans.get(i);
            log.debug(bean.toString());
            if ("AFTER".equalsIgnoreCase(bean.getType())) {
                if (i == beans.size() - 1) {
                    // 정정후 이후 끝까지..
                    String s = c.substring(bean.getIndex());
                    s = BizUtils.extractText(s);
                    log.debug("<<< " + s);
                    sb.append(s).append(" ");
                } else {
                    CorrectBean _bean = beans.get(i + 1);
                    log.debug("\t"+_bean.toString());
                    if ("BEFORE".equalsIgnoreCase(_bean.getType())) {
                        // 정정후 이후 정정전까지 데이터 추가.
                        String s = c.substring(bean.getIndex(), _bean.getIndex());
                        s = BizUtils.extractText(s);
                        log.debug("<<< " + s);
                        sb.append(s).append(" ");
                    }
                }
            } else {

            }
        }

    }

    private List<CorrectBean> findCorrectInfo(String c) {
        List<CorrectBean> beans = new ArrayList<>();
        String[] lines = StringUtils.splitByWholeSeparator(c, "\n");
        String[] lastLines = null;
        String lastLine = null;
        for (String line : lines) {
            line = line.trim();
            String[] _lines = BizUtils.getPlainText(line);
            // 먼저 라인에 정정전이나 정정후가 모두 있다고 하면..
            if (findBeforeCorrectKeyword(_lines) && findAfterCorrectKeyword(_lines)) {

                //
            } else if (findBeforeCorrectKeyword(_lines)) {
                if (isInTable(line)) {
                    log.debug("SKIP ::: " + line);
                    continue;
                }
                int correctIndex = StringUtils.indexOfAny(line, "정정 전", "정정전","정 정 전");
                if (correctIndex == -1) {
                    log.warn("SKIP ::: " + line);
                    continue;
                }
                if (correctIndex > 5) {
                    correctIndex = correctIndex - 5;
                }

                String subLine = line.substring(correctIndex);
                log.debug("BEFORE::: " + subLine.trim());
                beans.add(new CorrectBean("BEFORE", c.indexOf(subLine)));

            } else if (findAfterCorrectKeyword(_lines)) {
                if (isInTable(line)) {
                    log.debug("SKIP ::: " + line);
                    continue;
                }

                int correctIndex = StringUtils.indexOfAny(line, "정정 후", "정정후","정 정 후");
                if (correctIndex == -1) {
                    log.warn("SKIP ::: " + line);
                    continue;
                }
                if (correctIndex > 5) {
                    correctIndex = correctIndex - 5;
                }
                String subLine = line.substring(correctIndex);
                log.debug("AFTER::: " + subLine.trim());
                beans.add(new CorrectBean("AFTER", c.indexOf(subLine)));
            } else {

            }
        }
        // 정정전과 정정후가 같은 라인에 있을 때...
        if (beans.size() > 0) {
            CorrectBean last = beans.get(beans.size() - 1);
            if ("BEFORE".equalsIgnoreCase(last.getType())) {
                if (findAfterCorrectKeyword(lastLines)) {
                    log.debug("AFTER::: " + lastLine.trim());
                    // 정정전과 정정후가 같은 라인에 있으므로 정정후 이후부터 처리하려고 아래 로직 삽입.
                    int lastAfterCorrectIndex = StringUtils.indexOfAny(lastLine, "정정 후", "정정후");
                    beans.add(new CorrectBean("AFTER", c.indexOf(lastLine.substring(lastAfterCorrectIndex))));
                }
            }
        }

        return beans;

    }

    private boolean isInTable(String line) {
        if (line.contains("</TH>") || line.contains("</TD>")) {
            return true;
        } else {
            return false;
        }
    }


    private boolean findBeforeCorrectKeyword(String[] lines) {
        for (String line : lines) {
            line = StringUtils.remove(line, " ").trim();
            if (line.contains("정정전")) {
                return true;
            }
        }
        return false;
    }

    private boolean findAfterCorrectKeyword(String[] lines) {
        if (lines == null) {
            return false;
        }
        for (String line : lines) {
            line = StringUtils.remove(line, " ").trim();
            if (line.contains("정정후")) {
                return true;
            }
        }
        return false;
    }

    /**
     * 정정사항 테이블 찾아 정정후 데이터만 추가
     *
     * @param c
     * @param sb
     */
    private int findCorrectTable(String c, StringBuilder sb, String key) throws JSoupTableException {
        int index = findCorrectIndex(c);
        if (index == -1) {
            log.warn("정정사항 테이블이 없음. " + key);
            return index;
        }
        String _c = c.substring(index);
        Element tableElement = findCorrectTable(_c);
        if (tableElement == null) {
            return index;
        }

        // 버그 수정.

        //

        com.google.common.collect.Table<Integer, Integer, String> gtable = TreeBasedTable.create();
        findTitles(tableElement, gtable);
        findBodies(tableElement, gtable);
        List<List<String>> lists = JSoupHelper.convert(gtable);
        if (lists.size() >= 2) {
            List<String> titles = lists.get(0);
            int correctIndex = findCorrectField(titles);
            for (int i = 1; i < lists.size(); i++) {
                List<String> bodies = lists.get(i);
                if (bodies.size() - 1 >= correctIndex) {
                    String cc = bodies.get(correctIndex);
                    log.info("<< " + cc);
                    sb.append(cc).append(" ");
                }
            }
        }

        return index;
    }

    private int findCorrectField(List<String> titles) {
        for (int i = 0; i < titles.size(); i++) {
            String line = titles.get(i);
            line = BizUtils.removeBlank(line);
            if (line.contains("정정후")) {
                return i;
            }
        }
        return -1;
    }

    private Element findCorrectTable(String _c) {
        Document document = JSoupHelper.getDocumentByText(_c);
        Elements tables = JSoupHelper.findElements(document, "TABLE");
        if (tables != null && tables.size() != 0) {
            Element table = tables.first();
            return table;
        } else {
            return null;
        }
    }

    private int findCorrectIndex(String c) {
        String[] lines = StringUtils.splitByWholeSeparator(c, "\n");
        for (String line : lines) {
            String[] _lines = BizUtils.getPlainText(line);
            if (hasCorrect(_lines)) {
                return StringUtils.indexOf(c, line);
            }
        }

        return -1;
    }

    private boolean hasCorrect(String[] lines) {
        for (String line : lines) {
            line = StringUtils.remove(line, " ").trim();
            log.debug("정정사항 ... " + line);
            if (line.endsWith("정정사항")) {
                return true;
            }
        }

        return false;
    }

    private boolean isCorrectedKongsi(Map<String, Object> kongsiHodler) {
        String docNm = Maps.getValue(kongsiHodler, "doc_nm");
        return StringUtils.contains(docNm, "정정");
    }

    private boolean isOldAtCorrectedKongsi(Map<String, Object> kongsiHodler) {
        String rptNm = Maps.getValue(kongsiHodler, "rpt_nm");
        String docNm = Maps.getValue(kongsiHodler, "doc_nm");
        if (StringUtils.contains(rptNm, "정정")) {
            if (StringUtils.contains(docNm, "정정") == false) {
                return true;
            }
        }
        return false;
    }

    private String findKeyword(String c, List<String> telegramList) throws Exception {
        for (String keyword : telegramList) {
//            if (c.indexOf(keyword) >= 0) {
            if (hasKeyword(c, keyword)) {
                return keyword;
            }
        }
        return null;
    }

    private boolean hasKeyword(String c, String keyword) throws Exception {
        String conv = converter.convert(keyword);
        Stack<StackNode> evalStack = new Stack();
        Scanner scanner = new Scanner(conv);
        while (scanner.hasNext()) {
            String token = scanner.next();
            if (token.equals("+")) {
                String secondOperand = evalStack.pop().getData();
                boolean _second = executeCheck(c, secondOperand);
                String firstOperand = evalStack.pop().getData();
                boolean _first = executeCheck(c, firstOperand);
                String result = RESULT_PRIFIX + (_first || _second);
                evalStack.push(new StackNode(result));
            } else if (token.equals("!")) {
                String secondOperand = evalStack.pop().getData();
                boolean _second = executeCheck(c, secondOperand);
                String result = RESULT_PRIFIX + (!_second);
                evalStack.push(new StackNode(result));
            } else if (token.equals("*")) {
                String secondOperand = evalStack.pop().getData();
                boolean _second = executeCheck(c, secondOperand);
                String firstOperand = evalStack.pop().getData();
                boolean _first = executeCheck(c, firstOperand);
                String result = RESULT_PRIFIX + (_second && _first);
                evalStack.push(new StackNode(result));
            } else {
                boolean _second = executeCheck(c, token);
                String result = RESULT_PRIFIX + (_second);
                evalStack.push(new StackNode(result));
            }
        }
        boolean queryString = checkResult(evalStack.pop().getData());
        return queryString;
    }

    private boolean executeCheck(String c, String operand) {
        boolean result = false;
        if (isResult(operand)) {
            result = checkResult(operand);
        } else {
            result = execute(c, operand);
        }

        return result;
    }

    private boolean checkResult(String operand) {
        operand = StringUtils.remove(operand, RESULT_PRIFIX);
        return Boolean.valueOf(operand);
    }

    private boolean execute(String c, String operand) {
        return StringUtils.contains(c, operand);
    }


    private static boolean isResult(String value) {
        if (value.startsWith(RESULT_PRIFIX)) {
            return true;
        } else {
            return false;
        }
    }


    private boolean isSemiRealTimeData(Message message) {
        if (message.getMessageProperties().getHeaders().containsKey("__SEMI_REAL_TIME")) {
            if ("YES".equalsIgnoreCase((String) message.getMessageProperties().getHeaders().get("__SEMI_REAL_TIME"))) {
                return true;
            }
        }

        return false;
    }
}
