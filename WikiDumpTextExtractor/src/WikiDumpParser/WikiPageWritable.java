package WikiDumpParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class WikiPageWritable implements Writable {
    private Text pageTitle = new Text();
    private Text pageText = new Text();

    public void setPageTitle(Text pageTitle) {
        this.pageTitle = pageTitle;
    }

    public Text getPageTitle() {
        return pageTitle;
    }

    public void setPageText(Text pageText) {
        this.pageText = pageText;
    }

    public Text getPageText() {
        return pageText;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pageTitle.write(dataOutput);
        pageText.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pageTitle.readFields(dataInput);
        pageText.readFields(dataInput);
    }

    @Override
    public String toString() {
        return pageTitle.toString()
                + "███"
                + pageText.toString()
                ;
    }
}
