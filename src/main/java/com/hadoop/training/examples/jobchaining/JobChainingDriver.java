package com.hadoop.training.examples.jobchaining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class JobChainingDriver {


	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err
			.println("Usage: JobChainingDriver <posts> <users> <out>");
			System.exit(2);
		}
		
		Path postInput =  new Path(otherArgs[0]);
		Path userInput =  new Path(otherArgs[1]);
		Path outputDirIntermediate =  new Path(otherArgs[2] + "_init");
		Path outputDir =  new Path(otherArgs[2]);
		
		Job countingJob = new Job(conf, "JobChaining-Counting");
		countingJob.setJarByClass(JobChainingDriver.class);
		countingJob.setMapperClass(UserIdCountMapper.class);
		countingJob.setReducerClass(UserIdSumReducer.class);
		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(LongWritable.class);
		countingJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(countingJob, postInput);
		countingJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);
		
		int code = countingJob.waitForCompletion(true) ? 0 : 1;
		
		if (code == 0) {
			
			double numPosts = (double)countingJob.getCounters().findCounter(UserIdCountMapper.AVERAGE_CALC_GROUP, UserIdCountMapper.POSTS_COUNTER_NAME).getValue();
			double numUsers = (double)countingJob.getCounters().findCounter(UserIdCountMapper.AVERAGE_CALC_GROUP, UserIdSumReducer.USERS_COUNTER_NAME).getValue();
			
			double averagePostsPerUser = (double) numPosts/numUsers;
			
			Job binningJob = new Job(conf, "JobChaining-binning");
			binningJob.setJarByClass(JobChainingDriver.class);
			binningJob.setMapperClass(UserIdBinningMapper.class);
			UserIdBinningMapper.setAvgPostsPerUser(binningJob, averagePostsPerUser);
			binningJob.setNumReduceTasks(0);
			binningJob.setInputFormatClass(TextInputFormat.class);
			TextInputFormat.addInputPath(binningJob, outputDirIntermediate);
			
			MultipleOutputs.addNamedOutput(binningJob, UserIdBinningMapper.MULTIPLE_OUTPUTS_BELOW_NAME, TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(binningJob, UserIdBinningMapper.MULTIPLE_OUTPUTS_ABOVE_NAME, TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.setCountersEnabled(binningJob, true);
			
			TextOutputFormat.setOutputPath(binningJob, outputDir);
			
			
			FileStatus[] userFileStatus = FileSystem.get(binningJob.getConfiguration()).listStatus(userInput);
			for (FileStatus status : userFileStatus ) {
				
				DistributedCache.addCacheFile(status.getPath().toUri(), binningJob.getConfiguration());
				System.out.println("file: " + DistributedCache.getCacheFiles(binningJob.getConfiguration()));
			}
			
			code = binningJob.waitForCompletion(true) ? 0 : 1;
		}
		
		FileSystem.get(conf).delete(outputDirIntermediate, true);
		System.exit(code);
	}
}
