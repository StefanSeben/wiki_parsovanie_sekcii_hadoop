package WikiDumpParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WikiRedirectedSectionsFilter {
    public static class RedirectedSectionsFilter extends Mapper<Text, WikiRedirectedSectionWritable, Text, Text>
    {
        public void map(Text key, WikiRedirectedSectionWritable redirSection, Mapper.Context context
        ) throws IOException, InterruptedException {
            WikiPageWritable orgSection = null;

            if (redirSection.getRedirsCount() > 1) {
                for (WikiPageWritable section : redirSection.getRedirFrom()) {
                    if (section.getPageTitle().toString().equals(key.toString())) {
                        orgSection = section;
                        break;
                    }
                }

                if (orgSection != null) {
                    context.write(key, orgSection.getPageText());
                }
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
                System.err.println("Usage: wiki redirected sections filter <in> <out>");
                System.exit(2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Job job = Job.getInstance(conf, "wiki redirected sections filter");
            job.setJarByClass(WikiRedirectedSectionsFilter.class);
            job.setMapperClass(WikiRedirectedSectionsFilter.RedirectedSectionsFilter.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(WikiRedirSectionInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
