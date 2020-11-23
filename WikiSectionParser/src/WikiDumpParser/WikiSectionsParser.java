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

public class WikiSectionsParser {

    public static class SectionRedirectionsMapper extends Mapper<Text, WikiPageWritable, Text, WikiRedirectedSectionWritable> {

        public void map(Text key, WikiPageWritable parsedPage, Context context
        ) throws IOException, InterruptedException {
            List<Text> allMatches = new ArrayList<Text>();
            Matcher m = Pattern.compile("(?i)^(#redirect\\s*\\[\\[([^<>\\[#\\]]*#[^<>\\[#\\]]*)\\]\\])|(#presmeruj\\s*\\[\\[([^<>\\[#\\]]*#[^<>\\[#\\]]*)\\]\\])$")
                    .matcher(parsedPage.getPageText().toString());
            while (m.find()) {
                allMatches.add(new Text((m.group(2) == null ? m.group(4) : m.group(2))
                        .replaceAll("\\s", "")
                ));
            }

            WikiRedirectedSectionWritable redirSections = new WikiRedirectedSectionWritable();
            redirSections.addRedirFrom(parsedPage);

            for (Text match : allMatches) {
                context.write(match, redirSections);
            }

            if (allMatches.isEmpty())
                context.write(key, redirSections);
        }
    }

    public static class RedirectedSectionsReducer extends Reducer<Text,WikiRedirectedSectionWritable,Text,WikiRedirectedSectionWritable> {
        WikiRedirectedSectionWritable res;

        public void reduce(Text key, Iterable<WikiRedirectedSectionWritable> parsedRedirSections,
                           Context context
        ) throws IOException, InterruptedException {
            res = new WikiRedirectedSectionWritable();

            for (WikiRedirectedSectionWritable parsedRedirSection : parsedRedirSections) {
                res.addRedirFrom(parsedRedirSection);
            }

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
                System.err.println("Usage: wiki sections parser <in> <out>");
                System.exit(2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Job job = Job.getInstance(conf, "wiki sections parser");
            job.setJarByClass(WikiSectionsParser.class);
            job.setMapperClass(WikiSectionsParser.SectionRedirectionsMapper.class);
            job.setReducerClass(WikiSectionsParser.RedirectedSectionsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(WikiRedirectedSectionWritable.class);
            job.setInputFormatClass(WikiPageInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
