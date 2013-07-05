package com.wei;

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

public class PageRank {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        private URI[] vFileURI;
        private double []v;
        
        public void setup(Context context) throws IOException, InterruptedException {
            int now;
            double pr;
            Configuration conf = context.getConfiguration();
            vFileURI = DistributedCache.getCacheFiles(conf);
            Path vPath = new Path(vFileURI[0].toString());
            v = new double[6000000];
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(vPath)));
            String []words;
            String line;
            try {
                line = br.readLine();
                while (line != null){
                    words = line.split("\t");
                    if (words.length < 2) continue;
                    now = Integer.parseInt(words[0].trim());
                    pr = Double.parseDouble(words[1].trim());
                    v[now] = pr;
                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int num = 0;
	        List<Integer> numlist = new ArrayList<Integer>();
		    for (int i = 0; i < line.length(); ++i) {
		        if (Character.isDigit(line.charAt(i))) {
		            num = 10 * num + (line.charAt(i) - '0');
		        }
		        else {
		            if (num != 0) {
		                numlist.add(num);
		                num = 0;
		            }
		        }
		    }
		    if (numlist.size() < 2) {
		        context.write(new IntWritable(numlist.get(0)), new DoubleWritable(0));
		        return;
		    }
		
		    //numlist.add(num);
		    double pr;
		    int out = numlist.size();
		    for (int j = 1; j < out - 1; j = j + 2) {
		        pr = v[numlist.get(j)] / (double)numlist.get(j + 1);
		        System.out.printf("map %d: %f\n", numlist.get(j), pr);
		        
		        context.write(new IntWritable(numlist.get(0)), new DoubleWritable(pr));
		    }	
        }
    }
    
    public static class Combine extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        public void combine(IntWritable key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            System.out.printf("combine %d: %f %f\n", key.get(), sum);
            context.write(key, new DoubleWritable(sum));
        }
    }


    public static class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        private final static long number = 5716808;
        private final static double beta = 0.85D;
        private final static double init = 1.0D / number;
        private final static double ombeta = 1 - beta;
        private final static double ombetainit = ombeta * init;
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            System.out.printf("reduce %d: %f %f\n", key.get(), sum, sum * beta);
            sum = sum * beta + ombetainit;
            System.out.printf("reduce %d: %f %f\n", key.get(), sum, ombetainit);
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		
        DistributedCache.addCacheFile(new URI("/user/hadoop/" + args[1] + "/part-r-00000"), conf);

        Job job = new Job(conf, "pagerank");
        job.setJarByClass(PageRank.class);

	    job.setNumReduceTasks(3);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
	    job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
