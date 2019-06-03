package com.tachyon.news.camel.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Node;
import org.jsoup.select.NodeVisitor;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class StaffNodeVisitor implements NodeVisitor {
    private boolean onTable = false;
    private int index = 0;
    private List<String> strings = new ArrayList<>();
    private StringBuilder stringBuilder;
    @Override
    public void head(Node node, int i) {

        if (stringBuilder == null) {
            stringBuilder = new StringBuilder();
        }
        String name = node.nodeName();
        if (onTable == false) {
            if (name.contains("text")) {
                String _node = node.toString();
                _node = StringUtils.remove(_node, "&nbsp;");
                if ("".equalsIgnoreCase(_node.trim())) {

                } else {
                    stringBuilder.append(_node).append("\n");
                }
            }
        }else {
            if (name.contains("text")) {
            }
        }

        if (name.equalsIgnoreCase("TABLE")) {
            onTable = true;

            log.info(">> " + stringBuilder.toString().trim());
            strings.add(stringBuilder.toString().trim());
            stringBuilder = null;
        } else {


        }

    }

    @Override
    public void tail(Node node, int i) {
        String name = node.nodeName();
        if (name.equalsIgnoreCase("TABLE")) {
            onTable = false;
            if (stringBuilder == null) {
                stringBuilder = new StringBuilder();
            }
        }

    }

    public List<String> getStrings() {
        return strings;
    }
}
