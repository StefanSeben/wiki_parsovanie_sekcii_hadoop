package WikiDumpParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.StringReader;


public class WikiDumpPageExtractor {

    public static class TextMapper extends Mapper<LongWritable, Text, Text, WikiPageWritable>
    {
        private static WikiPageWritable parsedPage;

        public static class PageParserHandler extends DefaultHandler {
            boolean bTitle = false;
            boolean bText = false;

            String pageText = "";
            String title = "";

            Context context;

            public PageParserHandler(Context context) {
                this.context = context;
            }

            @Override
            public void startElement(
                    String uri, String localName, String qName, Attributes attributes) {

                if (qName.equalsIgnoreCase("title")) {
                    bTitle = true;
                } else if (qName.equalsIgnoreCase("text")) {
                    bText = true;
                    pageText = "";
                } else {
                    bText = false;
                    bTitle = false;
                }
            }

            @Override
            public void endElement(String uri,
                                   String localName, String qName) {

                if (qName.equalsIgnoreCase("title")) {
                    bTitle = false;
                } else if (qName.equalsIgnoreCase("text")) {
                    bText = false;
                    try {
                        if (pageText.isEmpty())
                            pageText = " ";

                        parsedPage = new WikiPageWritable();
                        parsedPage.setPageTitle(new Text(title));
                        parsedPage.setPageText(new Text(pageText.replaceAll("\\n", " ")));

                        context.write(parsedPage.getPageTitle(), parsedPage);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void characters(char ch[], int start, int length) {
                if (bText) {
                    pageText += new String(ch, start, length);
                } else if (bTitle) {
                    title = new String(ch, start, length);
                }
            }
        }

        public void map(LongWritable key, Text text, Context context
        ) throws IOException {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            try {
                SAXParser saxParser = factory.newSAXParser();
                PageParserHandler userhandler = new PageParserHandler(context);
                saxParser.parse(new InputSource(new StringReader(text.toString())), userhandler);
            } catch (SAXException | ParserConfigurationException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = null;
        try {
            parser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = parser.getRemainingArgs();
            if ((remainingArgs.length != 2)) {
                System.err.println("Usage: wiki page text extractor <in> <out>");
                System.exit(2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        try {
            Job job = Job.getInstance(conf, "wiki page text extractor");
            job.setJarByClass(WikiDumpPageExtractor.class);
            job.setMapperClass(TextMapper.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(WikiPageWritable.class);
            job.setInputFormatClass(XmlInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
