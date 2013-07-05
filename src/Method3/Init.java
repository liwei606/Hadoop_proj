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

public class Init {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		    String line = value.toString();
		
		    int num = 0;
	        List<Integer> numlist = new ArrayList<Integer>();
		    for (int i = 0; i < line.length(); ++i) {
		        if (Character.isDigit(line.charAt(i))) {
		            num = 10 * num + (line.charAt(i) - '0');
		        }
		        else if (line.charAt(i) == ':') {}
		        else {
		            numlist.add(num);
		            num = 0;
		        }
		    }
		    if (numlist.size() < 1) {
		        context.write(new IntWritable(num), new Text(""));
		        return;
		    }
			context.write(new IntWritable(numlist.get(0)), new Text(""));
		    numlist.add(num);
		
		    int out = numlist.size();
		    for (int j = 1; j < out; ++j) {
		        context.write(new IntWritable(numlist.get(j)), new Text(String.valueOf(numlist.get(0)) + " " + String.valueOf(out - 1)));
		    }	
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
            String sourcelist = "";
            for (Text val : values) {
		        sourcelist += val.toString();
		        sourcelist += " ";
            }
            context.write(key, new Text(sourcelist));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
	
        Job job = new Job(conf, "Init");
        job.setNumReduceTasks(3);
	    job.setJarByClass(Init.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
