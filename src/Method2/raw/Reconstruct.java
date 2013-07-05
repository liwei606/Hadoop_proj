package com.yumaoshu;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Reconstruct {
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        private Text output = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //System.out.println(line);
            String []words = line.split(":");
            if (words.length < 2) {
                words = line.split("\t");
            }
            if (words.length < 2) {
                return;
            }
            output.set(words[1]);
            context.write(new LongWritable(Long.parseLong(words[0])), output);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        private Text output = new Text();
        public void reduce(LongWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
            String pr = "", links = "";
            //System.out.print("key:" + key.get() + "\nvalues: ");
            for (Text val : values) {
                //System.out.print(" " + val.get());
                if (val.toString().charAt(0) == ' ') {
                    links = val.toString();
                } else {
                    pr = val.toString();
                }
            }
            //System.out.println("pr:" + pr + "links:" + links);
            output.set(':' + pr + links);
            context.write(key, output);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "reconstruct");
        job.setJarByClass(Reconstruct.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
