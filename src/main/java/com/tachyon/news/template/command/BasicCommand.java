package com.tachyon.news.template.command;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.model.Change;
import com.tachyon.crawl.kind.model.Kongsi;
import com.tachyon.crawl.kind.model.Staff;
import com.tachyon.crawl.kind.model.Table;
import com.tachyon.crawl.kind.util.JSoupHelper;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.BaseObject;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.model.DocNoIsuCd;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class BasicCommand extends BaseObject implements Command {
//    Map<String, Object> findKongsiHalder(TemplateMapper templateMapper, String docNo) {
//        return templateMapper.findKongsiHalder(docNo);
//    }

    Map<String, Object> findKongsiHalder(MyContext myContext, Message message, TemplateMapper templateMapper, String docNo, String code, String acptNo) {

        Map<String, Object> map = myContext.findKongsiMap(message);
        if (map == null) {
            map = findKongsiHalder(templateMapper, docNo, code, acptNo);
        }

        return map;
    }
    boolean isMajorStockChangeKongis(String docNm) {
        return docNm.contains("최대주주등소유주식변동신고서");
    }
    Map<String, Object> findKongsiHalder(TemplateMapper templateMapper, String docNo, String code, String acptNo) {
        return templateMapper.findKongsiHalder2(docNo, code, acptNo);
    }

    public void saveFile(String filePath, String content) throws IOException {
        log.info("saveFile =" + filePath);
        FileUtils.write(new File(filePath), content, "UTF-8", false);
    }

    /**
     * 파일 내용 찾기..
     *
     * @param htmlTargetPath
     * @param docNo
     * @param code
     * @param docUrl
     * @return
     * @throws IOException
     */
    String findDocRow(String htmlTargetPath, String docNo, String code, String docUrl, LoadBalancerCommandHelper loadBalancerCommandHelper) throws IOException {
        // 파일시스템에 있는지 확인
        String filePath = filePath(htmlTargetPath, docNo, code);
        log.info("filePath " + filePath);
        File file = new File(filePath);
        if (file.exists()) {
            return FileUtils.readFileToString(file, "UTF-8");
        }

        return findDocRow(docUrl, loadBalancerCommandHelper);
    }

    public String convertText(String docRaw) {
        return BizUtils.extractText(docRaw);
    }

    public String findTextFilePath(String htmlTargetPath, String code, String docNo) {
        return findPath(htmlTargetPath, code, docNo, "txt");
    }

    String findDocRow(String docUrl, LoadBalancerCommandHelper loadBalancerCommandHelper) throws IOException {

        // 파일시스템에 있는지 확인
        return findBody(loadBalancerCommandHelper, docUrl);
    }

    protected String findBody(LoadBalancerCommandHelper loadBalancerCommandHelper, String docUrl) throws IOException {
        if (loadBalancerCommandHelper == null) {
            return JSoupHelper.findBody(docUrl);
        } else {
            return loadBalancerCommandHelper.findBody(docUrl);
        }
    }

    public String filePath(String htmlTargetPath, String docNo, String isuCd) {
        return findPath(htmlTargetPath, isuCd, docNo, "htm");
    }

    String findFilePath(String htmlTargetPath, String code, String docNo) {
        return findPath(htmlTargetPath, code, docNo, "htm");
    }

    String findPath(String htmlTargetPath, String code, String docNo, String ext) {
        return BizUtils.htmlFilePath(htmlTargetPath, docNo, code, ext);
    }

    private String findUrl(String staticUrl, String docNo, String code) {
        if ("A-".equalsIgnoreCase(code)) {
            code = "SPACE";
        }
        String _url = BizUtils.htmlFilePath("html", docNo, code, "htm");
        if (staticUrl.endsWith("/")) {
            return staticUrl + _url;
        } else {
            return staticUrl + "/" + _url;
        }
    }

    String findBody(String docUrl) {
        try {
            return JSoupHelper.findBody(docUrl);
        } catch (IOException e) {
            return "";
        }
    }


    /**
     * @param key docNo_isuCd
     * @return
     */
    DocNoIsuCd find(String key) {
        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(key, "_");
        if (strings.length == 3) {
            return new DocNoIsuCd(strings[0], strings[1], strings[2]);
        } else if (strings.length == 2) {
            return new DocNoIsuCd(strings[0], strings[1], "");
        } else {
            return null;
        }
    }

//    public void insertStockHolder(TemplateMapper templateMapper, Map<String, Object> map) {
//        templateMapper.insertStockHolder(map);
//    }

    public boolean isChangeKongsi(String name) {
        if (name.contains("정정")) {
            return true;
        } else {
            return false;
        }
    }



    protected boolean isEmpty(String value) {
        return StringUtils.isEmpty(value);
    }


    public String findTnsDt(Map<String, Object> map) {
        String tndDt = Maps.getValue(map, "tns_dt");
        String acptNo = Maps.getValue(map, "acpt_no");
        return BizUtils.findTnsDt(tndDt, acptNo);
    }

    public void addTextFiles(Iterator<File> iterator, List<File> files) {
        while (iterator.hasNext()) {
            File f = iterator.next();
            String name = FilenameUtils.getExtension(f.getAbsolutePath());
            if ("txt".equalsIgnoreCase(name)) {
                files.add(f);
            }
        }
    }


    public Map<String, Object> paramMap(Kongsi kongsi, Kongsi.UrlInfo urlInfo, String isuCd, String day) {
        return urlInfo.simpleParamMap(kongsi.getAcpt_no(), isuCd, tnsDt(kongsi.getTns_dt(), day), kongsi.getRpt_nm(),kongsi.getSubmit_oblg_nm());
    }

    private String tnsDt(String tnsDt, String day) {
        if (tnsDt.length() == 4) {
            if (day.length() >= 8) {
                tnsDt = day.substring(0, 8) + tnsDt;
            } else {
                // 이런 경우는 없음..
                tnsDt = day + tnsDt;
            }
        }

        return tnsDt;
    }

    private String getTitle(Table table) {
        List<String> titles = table.getTitles();
        if (titles == null) {
            return table.getTitle();
        } else {
            if (titles.size() == 1) {
                return titles.get(0);
            } else {
                return table.getTitle();
            }
        }
    }

    protected String findTheDay(Table table, String docNo) {
        log.info("table " + table);
        String title = getTitle(table);
        if ("기준일".equalsIgnoreCase(title)) {
            List<List<String>> lists = table.getBodies();
            String _day = docNo.substring(0, 8);
            if (lists == null || lists.size() == 0) {
                return _day;
            } else {
                List<String> strings = lists.get(0);
                if (strings.size() == 0) {
                    return _day;
                } else {
                    String day = strings.get(0);
                    if ("".equalsIgnoreCase(day)) {
                        return _day;
                    } else {
                        return day;
                    }
                }
            }
        } else {
            return docNo.substring(0, 8);
        }

    }

    protected String findJsonBasicKongsiPath(String htmlTargetPath) {
        return StringUtils.replace(htmlTargetPath, "kongsi-new-html", "basic-kongsi");
    }


    protected boolean parsibleTitle(String title) {
        if ("임원".equalsIgnoreCase(title)) {
            return true;
        } else if ("미등기".equalsIgnoreCase(title)) {
            return true;
//        } else if ("변동".equalsIgnoreCase(title)) {
//            return true;
//        }else if ("겸직".equalsIgnoreCase(title)) {
//            return true;
        } else {
            return false;
        }
    }

    protected String findJsonBasicKongsiPath(String htmlTargetPath, String value) {
        String path = findJsonBasicKongsiPath(htmlTargetPath);
        if (path.endsWith("/")) {
            return path + value;
        } else {
            return path + "/" + value;
        }
    }

    protected String findFilePath(String htmlTargetPath, String companyCode) {
        if (htmlTargetPath.endsWith("/")) {
            return htmlTargetPath + companyCode;
        } else {
            return htmlTargetPath + "/" + companyCode;
        }
    }

    protected String findTxtFilePath(String htmlTargetPath, String docNo, String code) {
        if (htmlTargetPath.endsWith("/")) {
            return htmlTargetPath + code + "/" + docNo.substring(0, 4) + "/" + docNo + "_" + code + ".txt";
        } else {
            return htmlTargetPath + "/" + code + "/" + docNo.substring(0, 4) + "/" + docNo + "_" + code + ".txt";
        }
    }

    protected String findPath(String htmlTargetPath, String value) {
        if (value.startsWith("A")) {
            return findFilePath(htmlTargetPath, value);
        } else {
            if (value.contains(htmlTargetPath)) {
                return value;
            } else {
                //docNo_cd
                String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(value, "_");
                String docNo = strings[0].trim();
                String code = strings[1].trim();
                return findTxtFilePath(htmlTargetPath, docNo, code);
            }
        }
    }

    protected Collection<File> findBasicJonsFiles(String path) {

        File dir = new File(path);
        if (dir.exists() == false) {
            return null;
        }

        return FileUtils.listFiles(dir, FileFilterUtils.fileFileFilter(), FileFilterUtils.falseFileFilter());
    }

    protected File findBasicJonsFile(String path, String acptNo) {
        Collection<File> files = findBasicJonsFiles(path);
        File file = findBasicJonsFile(files, acptNo);
        if (file != null) {
            return file;
        } else {
            return findBasicJonsFile2(files, acptNo);
        }
    }

    private File findBasicJonsFile(Collection<File> files, String acptNo) {
        for (File file : files) {
            String name = FilenameUtils.getBaseName(file.getAbsolutePath());
            if (name.startsWith(acptNo)) {
                String[] strings = StringUtils.splitByWholeSeparator(name, "_");
                if ("SPACE".equalsIgnoreCase(strings[1]) == false) {
                    return file;
                }
            }
        }
        return null;
    }

    private File findBasicJonsFile2(Collection<File> files, String acptNo) {
        for (File file : files) {
            String name = FilenameUtils.getBaseName(file.getAbsolutePath());
            if (name.startsWith(acptNo)) {
                return file;
            }
        }
        return null;
    }

    protected void setupKongsiDay(Map<String, Staff> FILTER, String acptNo) {
        for (String key : FILTER.keySet()) {
            Staff staff = FILTER.get(key);
            String kongsiDay = staff.getKongsiDay();
            String parsed = BizUtils.parseDay(kongsiDay, acptNo);
//            String _kongsiDay = BizUtils.convertDay8(kongsiDay, parsed);
            staff.setNewKongsiDay(parsed);
            staff.setKongsiDay(parsed);
        }
    }

    /**
     * 이름이 - 인 것중에 이전 Change 성명과 동일하게 처리하는 로직..
     *
     * @param changes
     */
    protected void setupName(List<Change> changes) {
        if (hasNameDashNStock(changes)) {
            String name = findName(changes);
            if ("".equalsIgnoreCase(name)) {

            } else {
                log.info("- 이름을 가진 세부변동내역의 대표이름 " + name);
                if ("-".equalsIgnoreCase(name) || "\"".equalsIgnoreCase(name)) {
                    log.info("- 이름만 있으므로 처리하지 않음...");
                    changes.clear();
                } else {
                    log.info("- ==> " + name);
                    for (Change change : changes) {
                        change.setName(name);
                    }
                }
            }
        } else {
            // no action
        }
    }

    protected void setupName2(List<Change> changes) {

        String beforeName = "";
        for (Change change : changes) {
            String name = change.getName();
            if (isCopiableName(name)) {
                log.info(name + " ==> " + beforeName);
                change.setName(beforeName);

            } else {
                beforeName = name;
            }
        }
    }

    private boolean isCopiableName(String name) {
        if ("-".equalsIgnoreCase(name) || "\"".equalsIgnoreCase(name)) {
            return true;
        } else {
            return false;
        }
    }

    private String findName(List<Change> changes) {
        for (Change change : changes) {
            String name = change.getName();
            if (isEmpty(name) || "-".equalsIgnoreCase(name) || "\"".equalsIgnoreCase(name)) {

            } else {
                String _name = StringUtils.remove(name, " ");
                if ("합계".equalsIgnoreCase(_name)) {

                } else {
                    return name;
                }
            }
        }

        return "";
    }


    private boolean hasNameDashNStock(List<Change> changes) {
        for (Change change : changes) {
            if (change.getBefore() > 0 || change.getAfter() > 0) {
                String name = change.getName();
                if ("-".equalsIgnoreCase(name) || "\"".equalsIgnoreCase(name)) {
                    return true;
                }
            }
        }

        return false;
    }

}


