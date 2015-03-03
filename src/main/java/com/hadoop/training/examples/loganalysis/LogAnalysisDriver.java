package com.hadoop.training.examples.loganalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LogAnalysisDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: LogAnalysisDriver <log> <ClassB ipadd> <output dir>\n");
			System.exit(2);
		}

		Job job = new Job(conf);
		job.setJarByClass(LogAnalysisDriver.class);
		job.setJobName("LogAnalysisDriver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(LocationHourlyClicksMapper.class);
		job.setReducerClass(LocationHourlyClicksReducer.class);
		job.setCombinerClass(LocationHourlyClicksReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Add IP address to country Code Mapping to the DistributedCache
		DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(),
				job.getConfiguration());
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
