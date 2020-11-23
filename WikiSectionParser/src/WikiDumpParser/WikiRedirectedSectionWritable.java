package WikiDumpParser;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WikiRedirectedSectionWritable implements Writable {
    private List<WikiPageWritable> redirFrom = new ArrayList<>();

    public void setRedirFrom(List<WikiPageWritable> redirFrom) {
        this.redirFrom = redirFrom;
    }

    public void addRedirFrom(WikiPageWritable pageTitle) {
        this.redirFrom.add(pageTitle);
    }

    public void addRedirFrom(WikiRedirectedSectionWritable redirSections) {
        for (WikiPageWritable section : redirSections.getRedirFrom()) {
            addRedirFrom(section);
        }
    }

    public List<WikiPageWritable> getRedirFrom() {
        return redirFrom;
    }

    public int getRedirsCount() {
        return this.redirFrom.size();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(redirFrom.size());
        for (WikiPageWritable  section: redirFrom) {
            section.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        redirFrom = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            WikiPageWritable section = new WikiPageWritable();
            section.readFields(dataInput);
            redirFrom.add(section);
        }
    }

    @Override
    public String toString() {
        String redirFromString = "";

        for (WikiPageWritable section: redirFrom) {
            redirFromString += "███" + section.toString();
        }

        return redirFrom.size()
                + redirFromString
                ;
    }
}
