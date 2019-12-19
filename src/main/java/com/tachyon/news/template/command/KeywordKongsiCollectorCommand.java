package com.tachyon.news.template.command;

import com.google.common.collect.TreeBasedTable;
import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.CorrectBean;
import com.tachyon.crawl.kind.model.CorrectInfo;
import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.JSoupHelper;
import com.tachyon.crawl.kind.util.JSoupTableException;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.jobs.InfixToPostfixParens;
import com.tachyon.news.template.jobs.StackNode;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;

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
        log.info(key + " " + docUrl);
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
            log.debug("htmlFilePath=" + htmlFilePath);
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

            handleCorrectPart(c, sb, key);
            log.info("정정 " + sb.toString());
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

        kongsiHodler.put("doc_no", docNoIsuCd.getDocNo());
        // 사용자별 키워드가 없으므로 아래는 최대 하나 키워드가 존재..
        for (String keyword : keywords) {
            int count = templateMapper.findTelegramHolder(docNoIsuCd.getDocNo(), docNoIsuCd.getAcptNo(), keyword);
            if (count == 0) {
                templateMapper.insertTelegramHolder(docNoIsuCd.getDocNo(), docNoIsuCd.getIsuCd(), docNoIsuCd.getAcptNo(), keyword);

                setupParamMap(kongsiHodler, keyword, DateUtils.toString(new Date(), "yyyyMMddHHmm"));
                templateMapper.insertTrotHolder(kongsiHodler);
            } else {
                log.info("telegramHolder count " + count);
            }
        }

        log.info("done " + key);
    }

    private void setupParamMap(Map<String, Object> kongsiHodler, String keyword, String sigan) {
        kongsiHodler.put("keyword", keyword);
        kongsiHodler.put("sigan", sigan);
        kongsiHodler.put("date_sigan", sigan.substring(0,8));
    }

    /**
     * 정정공시 패턴을 구한다.
     * 패턴에 맞게 정정후 내용을 구한다.
     * 한테이블에 정정전, 정정후 내용이 있는 경우가 두가지가 있는데 구분해서 처리한다.
     *
     * @param c
     * @param sb
     * @param key
     * @throws JSoupTableException
     */
    private void handleCorrectPart(String c, StringBuilder sb, String key) throws Exception {
        c = c.toLowerCase();
        CorrectInfo correctInfo = findPattern(c);
        String pattern = correctInfo.getPattern();
        log.info("PATTERN "+pattern+" "+key);
        if ("SOMETABLE".equalsIgnoreCase(pattern)) {
            List<Element> elements = correctInfo.getElements();
            for (Element table : elements) {
                List<String> titles = findTitles(table);
                List<List<String>> bodies = findBodies(table);
                aggregate(sb, titles, bodies);
            }
        } else if ("DIVIDED".equalsIgnoreCase(pattern)) {
            List<CorrectBean> beans = findCorrectInfo(c);
            for (CorrectBean correctBean : beans) {
                log.info(correctBean.toString());
            }

            int index = 0;
            for (int i = 0; i < beans.size(); i++) {
                if (i < index) {
                    continue;
                }
                com.tachyon.crawl.kind.model.CorrectBean bean = beans.get(i);
                if ("AFTER".equalsIgnoreCase(bean.getType())) {
                    if (i == beans.size() - 1) {
                        // 정정후 이후 끝까지..
                        String s = c.substring(bean.getIndex());
                        s = BizUtils.extractText(s);
                        log.info(bean.getIndex()+"~ LAST <<< " + StringUtils.abbreviate(s,100));
                        sb.append(s).append(" ");
                    } else {
                        index = findBeforeIndex(beans, i);
                        if (index == -1) {
                            String s = c.substring(bean.getIndex());
                            s = BizUtils.extractText(s);
                            log.info(bean.getIndex()+" LAST <<< " + StringUtils.abbreviate(s,100));
                            sb.append(s).append(" ");
                            i = beans.size()-1;
                        } else {
                            CorrectBean _bean = beans.get(index);
                            // 정정후 이후 정정전까지 데이터 추가.
                            String s = c.substring(bean.getIndex(), _bean.getIndex());
                            s = BizUtils.extractText(s);
                            log.info(bean.getIndex()+" ~ "+_bean.getIndex()+ "<<< " + StringUtils.abbreviate(s,100));
                            sb.append(s).append(" ");
                            i = index;
                        }

                    }
                } else {

                }
            }

        } else if ("ONETABLE".equalsIgnoreCase(pattern)) {
            Element element = correctInfo.getElements().get(0);
            List<String> titles = findTitles(element);
            List<List<String>> bodies = findBodies(element);

            if (isOneField(titles)) {
                aggregateOneField(sb, titles, bodies);
            } else {
                aggregate(sb, titles, bodies);
            }
        } else {
            log.error("INVALID pattern " + pattern + " " + key);
        }

    }

    private int findBeforeIndex(List<CorrectBean> beans, int _i) {
        for (int i = _i; i < beans.size(); i++) {
            CorrectBean correctBean = beans.get(i);
            if ("BEFORE".equalsIgnoreCase(correctBean.getType())) {
                return i;
            }
        }

        return -1;
    }

    private boolean isOneField(List<String> titles) {
        for (String title : titles) {
            if (title.contains("정정전") && title.contains("정정후")) {
                return true;
            }
        }
        return false;
    }

    private void aggregateOneField(StringBuilder sb, List<String> titles, List<List<String>> bodies) {
        int index = findTitleIndex(titles);
        if (index == -1) {
            return;
        }
        for (List<String> body : bodies) {
            if (body.size() - 1 >= index) {
                String _body = body.get(index);
                _body = StringUtils.remove(_body, " ");
                sb.append(collectAfter(_body)).append(" ");
            } else {
                continue;
            }
        }
    }

    private String collectAfter(String body) {
        int index = body.indexOf("정정후");
        if (index != -1) {
            return body.substring(index);
        } else {
            return "";
        }
    }

    private List<com.tachyon.crawl.kind.model.CorrectBean> findCorrectInfo(String c) {
        List<com.tachyon.crawl.kind.model.CorrectBean> beans = new ArrayList<>();
        String[] lines = StringUtils.splitByWholeSeparator(c, "\n");
        String[] lastLines = null;
        String lastLine = null;
        int _beforeIndex = 0;
        int _afterIndex = 0;
        for (String line : lines) {
            line = line.trim();
            String[] _lines = BizUtils.getPlainText(line);
            // 먼저 라인에 정정전이나 정정후가 모두 있다고 하면..
            if (findBeforeCorrectKeyword(_lines) && findAfterCorrectKeyword(_lines)) {
                log.info("BEFORE_AFTER.. " + StringUtils.abbreviate(line,500));
                int beforeIndex = StringUtils.indexOfAny(line, "정정 전", "정정전", "정 정 전");
                if (beforeIndex == -1) {
                    continue;
                }
                if (beforeIndex > 5) {
                    beforeIndex = beforeIndex - 5;
                }
                int afterIndex = StringUtils.indexOfAny(line, "정정 후", "정정후", "정 정 후");
                if (afterIndex == -1) {
                    continue;
                }
                if (afterIndex > 5) {
                    afterIndex = afterIndex - 5;
                }

                if (beforeIndex > afterIndex) {
                    String subLine = line.substring(afterIndex, beforeIndex);
                    _afterIndex = c.indexOf(subLine, _afterIndex);
                    beans.add(new com.tachyon.crawl.kind.model.CorrectBean("AFTER", _afterIndex));
                    _afterIndex++;
                    subLine = line.substring(beforeIndex);
                    _beforeIndex = c.indexOf(subLine, _beforeIndex);
                    beans.add(new com.tachyon.crawl.kind.model.CorrectBean("BEFORE", _beforeIndex));
                    _beforeIndex++;
                } else {
                    String subLine = line.substring(beforeIndex, afterIndex);
                    _beforeIndex = c.indexOf(subLine, _beforeIndex);
                    beans.add(new com.tachyon.crawl.kind.model.CorrectBean("BEFORE", _beforeIndex));
                    _beforeIndex++;
                    subLine = line.substring(afterIndex);
                    _afterIndex = c.indexOf(subLine, _afterIndex);
                    beans.add(new com.tachyon.crawl.kind.model.CorrectBean("AFTER", _afterIndex));
                    _afterIndex++;
                }


                //
            } else if (findBeforeCorrectKeyword(_lines)) {
                if (isInTable(line)) {
                    log.debug("SKIP INTABLE ::: " + StringUtils.abbreviate(line,500));
                    continue;
                }
                int beforeIndex = StringUtils.indexOfAny(line, "정정 전", "정정전", "정 정 전");
                if (beforeIndex == -1) {
                    log.warn("SKIP ::: " + StringUtils.abbreviate(line,500));
                    continue;
                }
                if (beforeIndex > 5) {
                    beforeIndex = beforeIndex - 5;
                }

                String subLine = line.substring(beforeIndex);
                log.debug("BEFORE::: " + subLine.trim());
                _beforeIndex = c.indexOf(subLine, _beforeIndex);
                beans.add(new com.tachyon.crawl.kind.model.CorrectBean("BEFORE", _beforeIndex));
                _beforeIndex++;
            } else if (findAfterCorrectKeyword(_lines)) {
                if (isInTable(line)) {
                    log.debug("SKIP INTABLE ::: " + StringUtils.abbreviate(line,500));
                    continue;
                }

                int afterIndex = StringUtils.indexOfAny(line, "정정 후", "정정후", "정 정 후");
                if (afterIndex == -1) {
                    log.warn("SKIP ::: " + StringUtils.abbreviate(line,500));
                    continue;
                }
                if (afterIndex > 5) {
                    afterIndex = afterIndex - 5;
                }
                String subLine = line.substring(afterIndex);
                log.debug("AFTER::: " + subLine.trim());
                _afterIndex = c.indexOf(subLine, _afterIndex);
                beans.add(new com.tachyon.crawl.kind.model.CorrectBean("AFTER", _afterIndex));
                _afterIndex++;
            } else {

            }
        }
        // 정정전과 정정후가 같은 라인에 있을 때...
        if (beans.size() > 0) {
            com.tachyon.crawl.kind.model.CorrectBean last = beans.get(beans.size() - 1);
            if ("BEFORE".equalsIgnoreCase(last.getType())) {
                if (findAfterCorrectKeyword(lastLines)) {
                    log.debug("AFTER::: " + lastLine.trim());
                    // 정정전과 정정후가 같은 라인에 있으므로 정정후 이후부터 처리하려고 아래 로직 삽입.
                    int lastAfterCorrectIndex = StringUtils.indexOfAny(lastLine, "정정 후", "정정후");
                    beans.add(new com.tachyon.crawl.kind.model.CorrectBean("AFTER", c.indexOf(lastLine.substring(lastAfterCorrectIndex))));
                }
            }
        }

        return beans;

    }


    private int indexOfAny(String line, int before, String... strings) {
        for (String s : strings) {
            int index = line.indexOf(s, before);
            if (index != -1) {
                return index;
            }
        }

        return -1;
    }

    private List<String> findTitles(Element table) throws JSoupTableException {
        com.google.common.collect.Table<Integer, Integer, String> gtable = TreeBasedTable.create();
        JSoupHelper.parseTableHeader(table, gtable);
        return BizUtils.convertHeaders(JSoupHelper.convert(gtable));

    }

    private List<List<String>> findBodies(Element table) throws JSoupTableException {
        com.google.common.collect.Table<Integer, Integer, String> gtable = TreeBasedTable.create();
        JSoupHelper.parseTableBody(table, gtable);
        return JSoupHelper.convert(gtable);
    }

    private boolean isInTable(String line) {
        if (line.contains("</th>") || line.contains("</td>")) {
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

    private int findTitleIndex(List<String> titles) {
        for (int i = 0; i < titles.size(); i++) {
            String t = titles.get(i);
            if (t.contains("정정후")) {
                return i;
            }
        }
        return -1;
    }

    private void aggregate(StringBuilder sb, List<String> titles, List<List<String>> bodies) {
        int index = findTitleIndex(titles);
        if (index == -1) {
            return;
        }
        for (List<String> body : bodies) {
            if (body.size() - 1 >= index) {
                sb.append(body.get(index)).append(" ");
            } else {
                continue;
            }
        }
    }

    private CorrectInfo findPattern(String c) {
        Document document = Jsoup.parse(c);
        Element element = document.body();
        Elements elements = element.children();
        List<Element> list = findElements(elements);
        if (list.size() == 1) {
            return new CorrectInfo("ONETABLE", list);
        } else {
            int count = findTitle(list);
            if (count > 0) {
                return new CorrectInfo("DIVIDED", list);
            } else {
                return new CorrectInfo("SOMETABLE", list);
            }
        }
    }

    private int findTitle(List<Element> elements) {
        int count = 0;
        for (Element e : elements) {
            String s = e.nodeName();
            if (s.contains("table")) {

            } else {
                count++;
            }
        }
        return count;
    }

    private List<Element> findElements(Elements elements) {
        List<Element> list = new ArrayList<>();
        boolean isBelowCorrect = false;
        for (Element element : elements) {
            String name = element.nodeName();
            if (!"table".equalsIgnoreCase(name) && !"p".equalsIgnoreCase(name)) {
                continue;
            }

            String text = StringUtils.remove(element.text().trim(), " ");
            if ("p".equalsIgnoreCase(name) && "".equalsIgnoreCase(text)) {
                continue;
            }


            if ("p".equalsIgnoreCase(name) && text.contains("정정사항")) {
                isBelowCorrect = true;
            }
            if (isBelowCorrect == true) {
                if ("table".equalsIgnoreCase(name)) {
                    list.add(element);
                } else {
                    if ("p".equalsIgnoreCase(name) && (text.contains("정정전") || text.contains("정정후"))) {
                        list.add(element);
                    }
                }
            } else {
                continue;
            }
        }
        return list;

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



}
