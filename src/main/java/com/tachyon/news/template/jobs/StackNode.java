package com.tachyon.news.template.jobs;

/**
 * StackNode
 *
 * @author James Sonntag
 * @version v1.0.0
 */
public class StackNode
{
    private StackNode next;
    private String data;

    /**
     * Constructor for objects of class StackNode
     */
    public StackNode(String c)
    {
        data = c;
    }

    public void setNext(StackNode node) {
        next = node;
    }

    public StackNode getNext() {
        return next;
    }

    public String getData() {
        return data;
    }
}
