package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

import java.io.IOException;


public class WikiSectionParser
{
    public static class XmlReader extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public static IntWritable one = new IntWritable(1);
        public Text record = new Text();

        public void map(LongWritable key, Text text, Context context
        ) throws IOException, InterruptedException {

            context.write(text, one);
        }
    }


    public static class SectionParser extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable(0);

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main( String[] args )
    {
        Configuration conf = new Configuration();
        try {
            GenericOptionsParser parser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = parser.getRemainingArgs();
            if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
                System.err.println("Usage: wiki section parser <in> <out> [-skip skipPatternFile]");
                System.exit(2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Job job = Job.getInstance(conf, "wiki section parser");
            job.setJarByClass(WikiSectionParser.class);
            job.setMapperClass(XmlReader.class);
            job.setCombinerClass(SectionParser.class);
            job.setReducerClass(SectionParser.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            XmlInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
