package com.tachyon.news.template.jobs;

import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates an infix expression with parentheses
 * to a postfix expression.
 * 쓰레드세이프하지 않음.
 * @author Koffman & Wolfgang
 */
public class InfixToPostfixParens {

    // Nested Class

    /**
     * Class to report a syntax error.
     */
    public static class SyntaxErrorException
            extends Exception {

        /**
         * Construct a SyntaxErrorException with the specified
         * message.
         *
         * @param message The message
         */
        SyntaxErrorException(String message) {
            super(message);
        }
    }


    /**
     * The operators
     */
    private static final String OPERATORS = "+*!()";
    private static final char[] OPERATORSCHARS = OPERATORS.toCharArray();

    /**
     * The Pattern to extract tokens
     * A token is either a string of digits (\d+)
     * or a JavaIdentifier
     * or an operator
     */
    private static final Pattern pattern =
            Pattern.compile("\\d+\\.\\d*|\\d+|"
                    + "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*"
                    + "|[" + OPERATORS + "]");
    /**
     * The precedence of the operators, matches order of OPERATORS.
     */
    private static final int[] PRECEDENCE = {1, 2, 3, -1, -1};


    /**
     * 쌍따옴표 가운데 있는 데이터 추출..
     *
     */
    private final Pattern pattern2 = Pattern.compile("\"(.*?)\"");

    /**
     * Convert a string from infix to postfix.
     *
     * @param infix The infix expression
     * @throws SyntaxErrorException
     */
    public String convert(String infix) throws SyntaxErrorException {
        if (StringUtils.containsAny(infix, OPERATORSCHARS) == false) {
            return infix;
        }
        List<String> strings = findPatter2(infix);
        Stack<Character> operatorStack = new Stack<Character>();
        StringBuilder postfix = new StringBuilder();
        Scanner scan = new Scanner(infix);
        try {
            // Process each token in the infix string.
            String nextToken;
            while ((nextToken = scan.findInLine(pattern)) != null) {
                char firstChar = nextToken.charAt(0);
                // Is it an operand?
                if (Character.isJavaIdentifierStart(firstChar)
                        || Character.isDigit(firstChar)) {
                    postfix.append(nextToken);
                    postfix.append(' ');
                } // Is it an operator?
                else if (isOperator(firstChar)) {
                    processOperator(operatorStack,firstChar,postfix);
                } else {
                    throw new SyntaxErrorException("Unexpected Character Encountered: "
                            + firstChar);
                }
            } // End while.

            // Pop any remaining operators
            // and append them to postfix.

            if (operatorStack.empty()) {
                String s = postfix.toString().trim();
                return convert(StringUtils.replace(s," "," * "));
            } else {
                while (!operatorStack.empty()) {
                    char op = operatorStack.pop();
                    // Any '(' on the stack is not matched.
                    if (op == '(') {
                        throw new SyntaxErrorException(
                                "Unmatched opening parenthesis");
                    }
                    postfix.append(op);
                    postfix.append(" ");
                }

                String result = postfix.toString();
                result = modifyPattern2(strings, result);
                // assert: Stack is empty, return result.
                return result;

            }

        } catch (EmptyStackException ex) {
            throw new SyntaxErrorException("Syntax Error: The stack is empty");
        }
    }

    private String modifyPattern2(List<String> strings,String result) {
        if (strings.size() == 0) {
            return result;
        } else {
            for (String s : strings) {
                result = StringUtils.replace(result, s, "\"" + s+"\"");
            }
            return result;
        }
    }

    private List<String> findPatter2(String infix) {
        List<String> strings = new ArrayList<>();
        Matcher matcher = pattern2.matcher(infix);
        while (matcher.find()) {
            strings.add(matcher.group(1));
        }
        return strings;
    }
    /**
     * Method to process operators.
     *
     * @param op The operator
     * @throws EmptyStackException
     */
    private void processOperator(Stack<Character> operatorStack,char op,StringBuilder postfix) {
        if (operatorStack.empty() || op == '(') {
            operatorStack.push(op);
        } else {
            // Peek the operator stack and
            // let topOp be the top operator.
            char topOp = operatorStack.peek();
            if (precedence(op) > precedence(topOp)) {
                operatorStack.push(op);
            } else {
                // Pop all stacked operators with equal
                // or higher precedence than op.
                while (!operatorStack.empty()
                        && precedence(op) <= precedence(topOp)) {
                    operatorStack.pop();
                    if (topOp == '(') {
                        // Matching '(' popped - exit loop.
                        break;
                    }
                    postfix.append(topOp);
                    postfix.append(' ');
                    if (!operatorStack.empty()) {
                        // Reset topOp.
                        topOp = operatorStack.peek();
                    }
                }

                // assert: Operator stack is empty or
                //         current operator precedence >
                //         top of stack operator precedence.
                if (op != ')') {
                    operatorStack.push(op);
                }
            }
        }
    }

    /**
     * Determine whether a character is an operator.
     *
     * @param ch The character to be tested
     * @return true if ch is an operator
     */
    private boolean isOperator(char ch) {
        return OPERATORS.indexOf(ch) != -1;
    }

    /**
     * Determine the precedence of an operator.
     *
     * @param op The operator
     * @return the precedence
     */
    private int precedence(char op) {
        return PRECEDENCE[OPERATORS.indexOf(op)];
    }


    public static void main(String[] args) throws SyntaxErrorException {
        String k = "\"한국경제신문\"";
        InfixToPostfixParens infixToPostfixParens = new InfixToPostfixParens();
        System.out.println(infixToPostfixParens.convert(k));
    }
}
/*</listing>*/