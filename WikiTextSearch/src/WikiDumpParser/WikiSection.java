package WikiDumpParser;

import java.util.ArrayList;
import java.util.List;

public class WikiSection {
    private String pageTitle = "";
    private String sectionTitle = "";
    private String sectionText = "";
    private List<String> redirsFrom = new ArrayList<>();

    public WikiSection() {
    }

    public WikiSection(String pageTitle, String sectionTitle, String sectionText, String[] redirsFrom) {
        this.pageTitle = pageTitle;
        this.sectionTitle = sectionTitle;
        this.sectionText = sectionText;

        for (int i = 0; i < redirsFrom.length; ++i)
            this.redirsFrom.add(redirsFrom[i]);
    }

    public void setPageTitle(String pageTitle) {
        this.pageTitle = pageTitle;
    }

    public String getPageTitle() {
        return pageTitle;
    }

    public void setSectionTitle(String sectionTitle) {
        this.sectionTitle = sectionTitle;
    }

    public String getSectionTitle() {
        return sectionTitle;
    }

    public void setSectionText(String sectionText) {
        this.sectionText = sectionText;
    }

    public String getSectionText() {
        return sectionText;
    }

    public void setRedirsFrom(List<String> redirsFrom) {
        this.redirsFrom = redirsFrom;
    }

    public void addRedirFrom(String pageTitle) {
        redirsFrom.add(pageTitle);
    }

    public List<String> getRedirsFrom() {
        return redirsFrom;
    }

    public int getRedirCount() {
        return redirsFrom.size();
    }
}
