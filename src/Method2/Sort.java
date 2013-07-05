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

public class Sort {
    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, LongWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []words = line.split("\t");
            if (words.length < 2) {
                return;
            }
            context.write(new DoubleWritable(Double.parseDouble(words[1])), new LongWritable(Long.parseLong(words[0])));
        }
    }

    public static class Reduce extends Reducer<DoubleWritable, LongWritable, DoubleWritable, LongWritable> {
        public void reduce(DoubleWritable key, LongWritable value, Context context) 
            throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
