import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WikiSectionWritable implements Writable {
    private Text pageTitle = new Text();
    private Text sectionTitle = new Text();
    private Text text = new Text();
    private List<Text> redirsFrom = new ArrayList<>();

    public WikiSectionWritable() {
    }

    public WikiSectionWritable(Text pageTitle, Text sectionTitle, Text text) {
        this.pageTitle = pageTitle;
        this.sectionTitle = sectionTitle;
        this.text = text;
    }

    public WikiSectionWritable(Text title, Text text) {
        pageTitle = title;
        this.text = text;
    }

    public void setPageTitle(Text pageTitle) {
        this.pageTitle = pageTitle;
    }

    public Text getPageTitle() {
        return pageTitle;
    }

    public void setSectionTitle(Text sectionTitle) {
        this.sectionTitle = sectionTitle;
    }

    public Text getSectionTitle() {
        return sectionTitle;
    }

    public void setText(Text text) {
        this.text = text;
    }

    public Text getText() {
        return text;
    }

    public void setSection(WikiPage section) {
        setSectionTitle(new Text(section.getPageTitle()));
        setText(new Text(section.getPageText()));
    }

    public void setSection(WikiPageWritable section) {
        setSectionTitle(section.getPageTitle());
        setText(section.getPageText());
    }

    public void setRedirsFrom(List<Text> redirsFrom) {
        this.redirsFrom = redirsFrom;
    }

    public void addRedirsFrom(Text pageTitle) {
        this.redirsFrom.add(pageTitle);
    }

    public void addRedirsFrom(List<Text> sectionRedirs) {
        for (Text section : sectionRedirs) {
            addRedirsFrom(section);
        }
    }

    public void addRedirsFrom(WikiSectionWritable sectionRedirs) {
        addRedirsFrom(sectionRedirs.getRedirsFrom());
    }

    public List<Text> getRedirsFrom() {
        return redirsFrom;
    }

    public int getRedirsCount() {
        return this.redirsFrom.size();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pageTitle.write(dataOutput);
        sectionTitle.write(dataOutput);

        dataOutput.writeInt(redirsFrom.size());
        for (Text section: redirsFrom) {
            section.write(dataOutput);
        }

        text.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pageTitle.readFields(dataInput);
        sectionTitle.readFields(dataInput);

        int size = dataInput.readInt();
        redirsFrom = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            Text section = new Text();
            section.readFields(dataInput);
            redirsFrom.add(section);
        }

        text.readFields(dataInput);
    }

    @Override
    public String toString() {
        String redirFromString = "";

        for (Text section: redirsFrom) {
            redirFromString += "███" + section.toString();
        }

        return pageTitle.toString()
                + "███"
                + sectionTitle.toString()
                + "███"
                + redirsFrom.size()
                + redirFromString
                + "███"
                + text.toString()
                ;
    }
}