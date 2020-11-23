package WikiDumpParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiSectionExtractor {

    public static class SectionMapper extends Mapper<Text, WikiPageWritable, Text, WikiPageWritable>
    {
        public void map(Text key, WikiPageWritable parsedPage, Mapper.Context context
        ) throws IOException, InterruptedException {
            List<WikiPageWritable> allSections = new ArrayList<WikiPageWritable>();
            List<Text> allKeys = new ArrayList<Text>();

            String pageText = parsedPage.getPageText().toString();
            Matcher m = Pattern.compile("[^=;]==\\ ?([^=;]+)\\ ?==(?![=;])|^==\\ ?([^=;]+)\\ ?==(?![=;])")
                    .matcher(pageText);

            WikiPageWritable newSection = null;
            Text sectionTitle = null;
            int s = -1,e = -1;
            while (m.find()) {
                if (s != -1) {
                    e = m.start();

                    newSection.setPageText(new Text(pageText.substring(s, e)));

                    allSections.add(newSection);
                    allKeys.add(sectionTitle);
                }

                newSection = new WikiPageWritable();
                sectionTitle = new Text(
                        (parsedPage.getPageTitle()
                                + "#"
                                + m.group(1)
                        )
                                .replaceAll("\\s","")
                );

                newSection.setPageTitle(sectionTitle);

                s = m.end();
            }

            if (newSection != null) {
                newSection.setPageText(new Text(pageText.substring(s, pageText.length())));

                allSections.add(newSection);
                allKeys.add(sectionTitle);
            }
            else
                context.write(key, parsedPage);

            for (int i = 0; i < allSections.size(); ++i) {
                context.write(allKeys.get(i), allSections.get(i));
            }
        }
    }

    public static class DuplicationsFilter extends Reducer<Text,WikiPageWritable,Text,WikiPageWritable> {
        WikiPageWritable res;
        String pageText;

        public void reduce(Text key, Iterable<WikiPageWritable> parsedSections,
                           Context context
        ) throws IOException, InterruptedException {
            res = null;
            pageText = "";

            for (WikiPageWritable parsedSection : parsedSections) {
                if (res == null)
                    res = parsedSection;

                pageText += parsedSection.getPageText() + " ";
            }
            res.setPageText(new Text(pageText));

            context.write(key, res);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = null;
        try {
            parser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = parser.getRemainingArgs();
            if ((remainingArgs.length != 2)) {
                System.err.println("Usage: wiki sections extractor <in> <out>");
                System.exit(2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Job job = Job.getInstance(conf, "wiki sections extractor");
            job.setJarByClass(WikiSectionExtractor.class);
            job.setMapperClass(WikiSectionExtractor.SectionMapper.class);
            job.setReducerClass(WikiSectionExtractor.DuplicationsFilter.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(WikiPageWritable.class);
            job.setInputFormatClass(WikiPageInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
