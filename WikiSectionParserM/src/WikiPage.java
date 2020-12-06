public class WikiPage {
    private String pageTitle = "";
    private String pageText = "";

    public WikiPage() {
    }

    public WikiPage(String title, String text) {
        pageTitle = title;
        pageText = text;
    }

    public void setPageTitle(String pageTitle) {
        this.pageTitle = pageTitle;
    }

    public String getPageTitle() {
        return pageTitle;
    }

    public void setPageText(String pageText) {
        this.pageText = pageText;
    }

    public String getPageText() {
        return pageText;
    }


    @Override
    public String toString() {
        return pageTitle
                + "███"
                + pageText
                ;
    }
}
