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

public class PageRank {
    public static class Map extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //System.out.println(line);
            String []words = line.split(":");
            if (words.length == 1) {
                return;
            }
            //System.out.println(words[0]);
            //System.out.println("String []words.length " + words.length);
            String []tos = words[1].split(" ");
            LongWritable from = new LongWritable(Long.parseLong(words[0].trim()));
            context.write(from, new DoubleWritable(0));
            if (tos.length == 1) {
                context.write(from, new DoubleWritable(Double.parseDouble(tos[0])));
                return;
            }
            double gain = Double.parseDouble(tos[0]) / (tos.length - 1);
            for (int i = 1; i < tos.length; i++) {
                //System.out.println("Write <" + tos[i] + ", " + gain + ">");
                context.write(new LongWritable(Long.parseLong(tos[i])), new DoubleWritable(gain));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
        private final static long number = 100;
        private final static double beta = 0.85D;
        private final static double init = 1.0D / number;
        private final static double ombeta = 1 - beta;
        private final static double ombetainit = ombeta * init;
        public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
            double sum = 0;
            //System.out.print("key:" + key.get() + "\nvalues: ");
            for (DoubleWritable val : values) {
                //System.out.print(" " + val.get());
                sum += val.get();
            }
            //System.out.println();
            sum = sum * beta + ombetainit;
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "pagerank");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
