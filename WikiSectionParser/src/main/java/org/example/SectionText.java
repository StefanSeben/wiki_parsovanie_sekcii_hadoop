package org.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SectionText implements Writable {
    public String redirected_from;
    public String title;
    public String text;

    public SectionText() {}
    public SectionText(String redirected_from, String title, String text)
    {
        this.redirected_from = redirected_from;
        this.title = title;
        this.text = text;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(redirected_from);
        out.writeUTF(title);
        out.writeUTF(text);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        redirected_from = in.readUTF();
        title = in.readUTF();
        text = in.readUTF();
    }

    public static SectionText read(DataInput in) throws IOException {
        SectionText secText = new SectionText();
        secText.readFields(in);
        return secText;
    }
}
