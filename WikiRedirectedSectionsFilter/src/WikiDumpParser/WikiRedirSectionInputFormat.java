package WikiDumpParser;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class WikiRedirSectionInputFormat  extends FileInputFormat<Text, WikiRedirectedSectionWritable> {

    @Override
    public RecordReader<Text, WikiRedirectedSectionWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WikiRedirSectionInputFormat.WikiRedirSectionRecordReader recordReader = new WikiRedirSectionInputFormat.WikiRedirSectionRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);

        return recordReader;
    }

    public static class WikiRedirSectionRecordReader extends RecordReader<Text, WikiRedirectedSectionWritable> {
        private LineRecordReader lineRecordReader;

        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private Text currentKey;
        private WikiRedirectedSectionWritable currentValue;

        public WikiRedirSectionRecordReader() {
        }

        public void close() throws IOException {
            lineRecordReader.close();
        }

        public float getProgress() throws IOException {
            return lineRecordReader.getProgress();
        }

        public Text getCurrentKey() throws IOException, InterruptedException {
            return this.currentKey;
        }

        public WikiRedirectedSectionWritable getCurrentValue() throws IOException, InterruptedException {
            return this.currentValue;
        }

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!lineRecordReader.nextKeyValue()) {
                return false;
            }
            String line = lineRecordReader.getCurrentValue().toString();

            String[] valueFields = line.split("███");
            String[] keyFields = valueFields[0].split("\t");

            currentKey = new Text(keyFields[0]);
            currentValue = new WikiRedirectedSectionWritable();

            int redirsCount = Integer.parseInt(keyFields[1]);
            for (int i = 0; i < redirsCount; ++i) {
                WikiPageWritable section = new WikiPageWritable();
                section.setPageTitle(new Text(valueFields[1 + i * 2]));
                if (2 + i * 2 < valueFields.length)
                    section.setPageText(new Text(valueFields[2 + i * 2]));

                currentValue.addRedirFrom(section);
            }

            return true;
        }
    }
}
