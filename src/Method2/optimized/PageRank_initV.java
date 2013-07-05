package com.yumaoshu;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

public class PageRank_initV {
    //public static double v[10000000];
    public static class Map extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

        private URI[] vFileURI;
        private double []v;
        public void setup(Context context) throws IOException, InterruptedException {
            //System.out.println("in setup: reading v");
            int now;
            double pr;
            Configuration conf = context.getConfiguration();
            vFileURI = DistributedCache.getCacheFiles(conf);
            Path vPath = new Path(vFileURI[0].toString());
            v = new double[6000000];
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(vPath)));
            String []words;
            try {
                String line;
                line = br.readLine();
                while (line != null){
                    //System.out.println(line);
                    words = line.split("\t");
                    if (words.length < 2) continue;
                    now = Integer.parseInt(words[0].trim());
                    //context.write(new LongWritable(now), new DoubleWritable(0));
                    pr = Double.parseDouble(words[1].trim());
                    v[now] = pr;
                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
            //System.out.println("v1 is " + v[1] + " v2 is " + v[2]);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String line = value.toString();
            //System.out.println(line);
            line += " ";
            String []words = line.split(":");
            if (words.length < 2) {
                return;
            }
            //System.out.println("words[0] " + words[0]);
            //System.out.println("String []words.length " + words.length);
            String []tos = (words[1].trim() + " ").split(" ");
            //System.out.println("String []tos.length " + tos.length);
            //System.out.println("tos[0] " + tos[0]);
            long fromLong = Long.parseLong(words[0].trim());
            LongWritable from = new LongWritable(fromLong);
            context.write(from, new DoubleWritable(0));
            if (tos.length == 0) {
                context.write(from, new DoubleWritable(v[(int)fromLong]));
                return;
            }
            double gain = v[(int)fromLong] / tos.length;
            for (int i = 0; i < tos.length; i++) {
                //System.out.println("Write <" + tos[i] + ", " + gain + ">");
                context.write(new LongWritable(Long.parseLong(tos[i])), new DoubleWritable(gain));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
        private final static long number = 5716808;
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
        DistributedCache.addCacheFile(new URI("/user/hadoop/" + args[1] + "/part-r-00000"), conf);

        Job job = new Job(conf, "PageRank_initV");
        job.setJarByClass(PageRank_initV.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
