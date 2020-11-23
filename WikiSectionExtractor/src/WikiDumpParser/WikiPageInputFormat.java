package WikiDumpParser;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class WikiPageInputFormat extends FileInputFormat<Text, WikiPageWritable> {

    @Override
    public RecordReader<Text, WikiPageWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WikiPageRecordReader recordReader = new WikiPageRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);

        return recordReader;
    }

    public static class WikiPageRecordReader extends RecordReader<Text, WikiPageWritable> {
        private LineRecordReader lineRecordReader;

        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private Text currentKey;
        private WikiPageWritable currentValue;

        public WikiPageRecordReader() {
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

        public WikiPageWritable getCurrentValue() throws IOException, InterruptedException {
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

            currentKey = new Text(keyFields[1]);
            currentValue = new WikiPageWritable();

            currentValue.setPageTitle(new Text(keyFields[1]));
            currentValue.setPageText(new Text(valueFields.length > 1 ? valueFields[1] : ""));

            return true;
        }
    }
}

