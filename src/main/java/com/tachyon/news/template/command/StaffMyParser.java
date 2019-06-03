package com.tachyon.news.template.command;

import com.google.common.collect.TreeBasedTable;
import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.util.JSoupHelper;
import com.tachyon.crawl.kind.util.JSoupTableException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeTraversor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.tachyon.crawl.BizUtils.findBodies;
import static com.tachyon.crawl.BizUtils.findTitles;


@Slf4j
public class StaffMyParser extends AbstractMyParser {

    private static Pattern DIGIT_PATTERN = Pattern.compile("(\\d+)");
    private SimpleContext simpleContext;

    public SimpleContext getSimpleContext() {
        return simpleContext;
    }

    public void setSimpleContext(SimpleContext simpleContext) {
        this.simpleContext = simpleContext;
    }

    private void handleTable(Elements _tables, List<Table> tables, List<String> titles) throws JSoupTableException {
        int index = 0;
        int count = 0;
        boolean doFirstTable = false;
        for (Element _table : _tables) {
            log.info(index + " table ... ");
            int _index = index;
            if (index++ == 0) {
                if (tables.size() == 0) {
                    // 기준일 테이블이 없는 경우..
                } else {
                    // 기준일 테이블이 있는 경우..
                    log.info("skip  table ");
                    continue;
                }
            }
            String text = _table.text().trim();
            Table table = new Table();
            com.google.common.collect.Table<Integer, Integer, String> gtable = TreeBasedTable.create();
            findTitles(_table, gtable);
            findBodies(_table, gtable);
            List<List<String>> lists = JSoupHelper.convert(gtable);
            count += lists.size();
            whenHeader0(lists, table);
            if (table.getTitles() == null || table.getTitles().size() == 0) {
                //테이블에 타이틀이 있는 경우
                log.info("skip table text " + text);
                if (doFirstTable == true) {
                    // 처음 임원테이블이 지난 다음..
                    if (text.contains("상기") == false) {
                        if ("UNKNOWN".equalsIgnoreCase(findTypeFromString2(text)) == false) {
                            String _title = titles.get(_index);
                            titles.set(_index, _title + " " + text);
                            log.info("set title " + _index + " " + _title + " " + text);
                        }
                    }
                }
                table.setTitle("SKIP");
                tables.add(table);
            } else {
                doFirstTable = true;
                log.info("setTitle OK ");
                table.setTitle("OK");
                tables.add(table);
            }
        }
        if (getSimpleContext() != null) {
            simpleContext.setCount(count);
        }
    }

    /**
     * 헤더
     * 헤더
     * 바디
     * 이런식으로 되므로.. 헤더를 분리해내는게 주요 로직...
     *
     * @param bodies
     * @param table
     */
    private void whenHeader0(List<List<String>> bodies, Table table) {
        // 헤더
        // 헤더
        // 바디

        if (bodies.size() == 0) {
            log.info("테이블열이 없음.. ");
            return;
        }
        int index = 0;
        for (List<String> list : bodies) {
            log.debug((index++) + " " + list);
        }

        int bodyIndex = findBodyIndex(bodies);
        if (bodyIndex == -1) {
            return;
        }
        log.debug("bodyIndex " + bodyIndex);
        if (bodyIndex == 1) {
            int size = bodies.size();
            for (int i = 0; i < size; i++) {
                List<String> strings = bodies.get(i);
                if (i == 0) {
                    table.setTitles(strings);
                } else {
                    table.add(strings);
                }
            }
        } else {
            List<String> header = findHeader(bodies, bodyIndex);
            int size = bodies.size();
            for (int i = 0; i < size; i++) {
                List<String> strings = bodies.get(i);
                if (i >= bodyIndex) {
                    log.debug("BODY >> " + strings);
                    table.add(strings);
                }
            }
            log.info("header >> " + header);
            table.setTitles(header);
        }
    }

    private List<String> findHeader(List<List<String>> bodies, int bodyIndex) {
        List<String> header = bodies.get(0);
        aggregate(header);
        if (bodyIndex == 1) {
            return header;
        }

        for (int i = 1; i < bodies.size(); i++) {
            if (i < bodyIndex) {
                List<String> strings = bodies.get(i);
                aggregate2(header, strings);
                log.debug("<< " + strings.toString());
            }
        }

        return header;
    }

    private void aggregate(List<String> header) {
        for (int i = 0; i < header.size(); i++) {
            String v = header.get(i);
            v = BizUtils.removeBlank(v);
            header.set(i, v);

        }
    }

    private void aggregate2(List<String> result, List<String> values) {
        for (int i = 0; i < result.size(); i++) {
            String r = result.get(i);
            r = BizUtils.removeBlank(r);
            if (i > (values.size() - 1)) {
                break;
            }
            String v = values.get(i);
            v = BizUtils.removeBlank(v);
            if (r.equalsIgnoreCase(v)) {
                log.debug("same " + r + " " + v);
            } else {
                log.debug(r + " << " + v);
                result.set(i, r + v);
            }
        }
    }

    private int findBodyIndex(List<List<String>> bodies) {
        List<String> src = bodies.get(0);
        for (int i = 1; i < bodies.size(); i++) {
            List<String> trg = bodies.get(i);
            if (findSameNumber(src, trg) < baseNumber(bodies.size())) {
                return i;
            }
        }

        // 이런 경우는 없음.
        return -1;
    }

    private int baseNumber(int count) {
        if (count < 5) {
            return 2;
        } else {
            return 3;
        }
    }


    private int findSameNumber(List<String> src, List<String> trg) {
        int count = 0;
        for (int i = 0; i < src.size(); i++) {
            if (i > (trg.size() - 1)) {
                continue;
            }
            if (src.get(i).equalsIgnoreCase(trg.get(i))) {
                count++;
            }
        }
        log.debug("SRC " + src + " TRG " + trg + " " + count);
        return count;
    }

    @Override
    public List<Table> parse(Object o) throws Exception {
        Document _document = (Document) o;
        String html = _document.html();
        log.debug(html);
        log.debug("");
        List<String> titles = findAnyTitles(_document);
        List<Table> tables = new ArrayList<>();
        try {
            Elements _tables = JSoupHelper.findElements(_document, "TABLE");
            log.info("<< " + titles.size() + "\t" + _tables.size());
            if (_tables.size() == 0) {
                return tables;
            }
            handleKongsiDay(_tables, tables);
            handleTable(_tables, tables, titles);
            log.info(">> " + titles.size() + "\t" + tables.size());
            setupTableName(tables, titles);
            return tables;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

    }

    private void setupTableName(List<Table> tables, List<String> titles) throws Exception {

        boolean checkedStaff = false;
        for (int i = 0; i < tables.size(); i++) {
            Table table = tables.get(i);
            String title = table.getTitle();
            log.info(i + " >>>> " + title);
            if (title == null) {
                table.setTitle("SKIP");
                log.info("setTitle SKIP");
            } else if ("SKIP".equalsIgnoreCase(title)) {
                log.info("setTitle SKIP");
            } else if ("OK".equalsIgnoreCase(title)) {
                if (checkedStaff == false) {
                    checkedStaff = true;
                    log.info("setTitle 임원");
                    table.setTitle("임원");
                } else {

                    // 인덱스가 동일한 타이블을 가져옴.
                    if (titles.size() - 1 < (i)) {
                        continue;
                    }
                    String _title = titles.get(i);

                    String type = findSimpleType(_title);
                    log.info(type + " < == " + _title);
                    if ("UNKNOWN".equalsIgnoreCase(type)) {

                        // 인덱스가 하나 적은 타이틀을 가져옴.
                        if (titles.size() - 1 < (i - 1)) {
                            continue;
                        }
                        _title = titles.get(i - 1);
                        type = findSimpleType(_title);
                        if ("UNKNOWN".equalsIgnoreCase(type) == false) {
                            table.setTitle(type);
                        }

                    } else {
                        table.setTitle(type);
                    }
                }
            } else {

            }

            if ("UNKNOWN".equalsIgnoreCase(table.getTitle())) {
            }
        }
    }



    private String findTypeFromString2(String title) {
        if (title.contains("임원")) {
            if (title.contains("변동")) {
                return "변동";
            } else if (title.contains("겸직")) {
                return "겸직";
            } else if (title.contains("겸임")) {
                return "겸직";
            } else if (title.contains("비등기") || title.contains("미등기")) {
                return "미등기";
            } else if (title.contains("임원")) {
                return "임원";
            } else {
                return "UNKNOWN";
            }
        } else {
            if (title.contains("고문") || title.contains("자문")) {
                return "고문자문";
            } else if (title.contains("변동일")) {
                return "UNKNOWN";
            } else if (title.contains("변동")) {
                return "변동";
            } else {
                return "UNKNOWN";
            }
        }
    }

    private String findSimpleType(String title) {
        if (title.contains("변동") && (title.contains("비등기") || title.contains("미등기"))) {
            String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(title, "\n");
            return findSimpleType(strings);
        } else if (title.contains("변동")) {
            return "UNKNOWN";
        } else if (title.contains("비등기") || title.contains("미등기")) {
            return "미등기";

        } else {
            return "UNKNOWN";
        }
    }

    private String findSimpleType(String[] strings) {
        for (String s : strings) {
            if (s.contains("비등기") || s.contains("미등기")) {
                if (s.contains("변동")) {
                    return "변동";
                } else {
                    return "미등기";
                }
            }
        }
        return "UNKNOWN";
    }


    /**
     * 테이블 사이에 타이틀을 파싱한다.
     *
     * @param document
     */
    private void handleElements(Document document, StaffNodeVisitor staffNodeVisitor) {
        NodeTraversor.traverse(staffNodeVisitor, document); // walk the DOM, and call .head() and .tail() for each node

    }

    private void handleKongsiDay(Elements _tables, List<Table> tables) {
        Element _table = _tables.first();
        String t = _table.text().trim();
        handleKongsiDay(t, tables);
    }

    private void handleKongsiDay(String t, List<Table> tables) {
        t = BizUtils.removeBlank(t);
        if (t.contains("기준일")) {
            Table table = new Table("기준일");
            List<String> headers = new ArrayList<>();
            headers.add("기준일");
            table.setTitles(headers);
            List<List<String>> bodies = new ArrayList<>();
            List<String> strings = new ArrayList<>();
            String value = parseTheDay2(t);
            strings.add(value);
            bodies.add(strings);
            table.setBodies(bodies);
            table.setTitle("SKIP");
            tables.add(table);
        }
    }

//    public static String parseTheDay(String value) {
//        Matcher matcher = null;
//        if (value.contains("[")) {
//            matcher = PATTERN4.matcher(value);
//        } else {
//            matcher = PATTERN3.matcher(value);
//        }
//        if (matcher.find()) {
//            String _value = matcher.group(1);
//            int index = _value.indexOf(")");
//            if (index == -1) {
//                index = _value.indexOf("]");
//                if (index == -1) {
//                    return "";
//                } else {
//                    return _value.substring(0, index).trim();
//                }
//            } else {
//                return _value.substring(0, index).trim();
//            }
//        } else {
//            return "";
//        }
//    }

    private String parseTheDay2(String value) {
        StringBuilder builder = new StringBuilder();
        char[] chars = value.toCharArray();
        for (char c : chars) {
            if (Character.isSpaceChar(c)==false) {
                builder.append(c);
            }
        }

        return builder.toString();
    }

}
