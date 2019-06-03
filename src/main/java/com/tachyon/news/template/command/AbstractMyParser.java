package com.tachyon.news.template.command;

import com.tachyon.crawl.kind.parser.MyParser;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.nodes.Document;
import org.jsoup.select.NodeTraversor;

import java.util.List;

@Slf4j
public abstract class AbstractMyParser implements MyParser {
    public List<String> findAnyTitles(Document _document) {
        StaffNodeVisitor staffNodeVisitor = new StaffNodeVisitor();
        handleElements(_document, staffNodeVisitor);
        List<String> strings = staffNodeVisitor.getStrings();
        for (String s : strings) {
            log.debug("title " + s);
        }
        return strings;
    }
    private void handleElements(Document document, StaffNodeVisitor staffNodeVisitor) {
        NodeTraversor.traverse(staffNodeVisitor, document); // walk the DOM, and call .head() and .tail() for each node

    }

}
