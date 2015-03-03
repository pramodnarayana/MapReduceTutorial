package com.hadoop.training.examples.jobcontrol;

import java.io.IOException;

import com.hadoop.training.examples.jobcontrol.UserBinningMapper;
import com.hadoop.training.examples.jobcontrol.AveragePostsPerUserMapper;
import com.hadoop.training.examples.jobcontrol.AveragePostsPerUserReducer;

import com.hadoop.training.examples.jobcontrol.AverageReputationMapper;
import com.hadoop.training.examples.jobcontrol.AverageReputationReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;


public class JobControlDriver {
	
	public static void handleRun(JobControl control) throws InterruptedException {
	    JobRunner runner = new JobRunner(control);
	    Thread t = new Thread(runner);
	    t.start();

	    while (!control.allFinished()) {
	        System.out.println("Still running...");
	        Thread.sleep(5000);
	    }
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();


		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err
			.println("Usage: JobControlDriver <posts> <users> <out>");
			System.exit(2);
		}

		Path postInput = new Path(otherArgs[0]);
		Path userInput = new Path(otherArgs[1]);
		Path countingOutput = new Path(otherArgs[2] + "_count");
		Path binningOutputRoot = new Path(otherArgs[2] + "_bins");
		Path binningOutputBelow = new Path(binningOutputRoot + "/" + UserBinningMapper.MULTIPLE_OUTPUTS_BELOW_NAME);
		Path binningOutputAbove = new Path(binningOutputRoot + "/" + UserBinningMapper.MULTIPLE_OUTPUTS_ABOVE_NAME);
		Path aboveAvgReputationOutput = new Path(otherArgs[2] + "/" + UserBinningMapper.MULTIPLE_OUTPUTS_ABOVE_NAME);
		Path belowAvgReputationOutput = new Path(otherArgs[2] + "/" + UserBinningMapper.MULTIPLE_OUTPUTS_BELOW_NAME);

		Job countingJob = getCountingJob(conf, postInput, countingOutput);

		int code = 1;
		code = countingJob.waitForCompletion(true) ? 0 : 1;
		System.out.println("Counting Job code:" + code);
		if (code == 0) {
			
			System.out.println("Counting Job completed");
				
			
			Job binningJob = getBinningJob(conf, countingJob, countingOutput, userInput, binningOutputRoot);
			ControlledJob binningControlledJob = new ControlledJob(binningJob.getConfiguration());
            
			Job belowAvgReputationJob = getAverageJob(conf, binningOutputBelow, belowAvgReputationOutput);
			ControlledJob belowAvgRepContJob =  new ControlledJob(belowAvgReputationJob.getConfiguration());
			belowAvgRepContJob.addDependingJob(binningControlledJob);
			
            Job aboveAvgReputationJob = getAverageJob(conf, binningOutputAbove, aboveAvgReputationOutput);
            ControlledJob aboveAvgRepContJob = new ControlledJob(aboveAvgReputationJob.getConfiguration());
            aboveAvgRepContJob.addDependingJob(binningControlledJob);

			JobControl jc = new JobControl("Average Reputation");
			jc.addJob(binningControlledJob);
			jc.addJob(belowAvgRepContJob);
			jc.addJob(aboveAvgRepContJob);
			
			handleRun(jc);
			
			
		}
		
		System.out.println("All Done");
		System.exit(code);
	}

	public static Job getCountingJob(Configuration conf, Path inputDir, Path outputDir) throws IOException {

		Job countingJob  = new Job(conf, "JobChaining-Counting");
		countingJob.setJarByClass(JobControlDriver.class);

		// Set our mapper and reducer, we can use the API's long sum reducer for
		// a combiner!
		countingJob.setMapperClass(AveragePostsPerUserMapper.class);
		countingJob.setCombinerClass(LongSumReducer.class);
		countingJob.setReducerClass(AveragePostsPerUserReducer.class);

		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(LongWritable.class);

		countingJob.setInputFormatClass(TextInputFormat.class);

		TextInputFormat.addInputPath(countingJob, inputDir);

		countingJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(countingJob, outputDir);

		return countingJob;
	}
	
	public static Job getBinningJob(Configuration conf, Job countingJob, Path countingOutput, Path userInput, Path outputDir) throws IOException {
		
		double numRecords = (double)countingJob.getCounters().findCounter(AveragePostsPerUserMapper.AVERAGE_CALC_GROUP, AveragePostsPerUserMapper.POSTS_COUNTER_NAME).getValue();
		double numUsers = (double)countingJob.getCounters().findCounter(AveragePostsPerUserReducer.AVERAGE_CALC_GROUP, AveragePostsPerUserReducer.USERS_COUNTER_NAME).getValue();
		
		double averagePostsPerUser =  numRecords/numUsers;
		System.out.println("averagePostsPerUser:" + averagePostsPerUser);
		
		Job binningJob =  new Job(conf, "JobControl-Binning");
		binningJob.setJarByClass(JobControlDriver.class);
		binningJob.setMapperClass(UserBinningMapper.class);
		UserBinningMapper.setAvgPostsPerUser(binningJob, averagePostsPerUser);
		binningJob.setNumReduceTasks(0);
		binningJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(binningJob, countingOutput);
		
		MultipleOutputs.addNamedOutput(binningJob, UserBinningMapper.MULTIPLE_OUTPUTS_BELOW_NAME, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(binningJob, UserBinningMapper.MULTIPLE_OUTPUTS_ABOVE_NAME, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.setCountersEnabled(binningJob, true);
		
		TextOutputFormat.setOutputPath(binningJob, outputDir);
		
				
		FileStatus[] userFileStatus = FileSystem.get(conf).listStatus(userInput);
		for (FileStatus status : userFileStatus ) {
			
			DistributedCache.addCacheFile(status.getPath().toUri(), binningJob.getConfiguration());
			
		}

		return binningJob;
	}
	
	public static Job getAverageJob(Configuration conf, Path inputDir, Path outputDir) throws IOException {
		Job averageJob = new Job(conf, "ParallelJobs");
		averageJob.setJarByClass(JobControlDriver.class);

		averageJob.setMapperClass(AverageReputationMapper.class);
		averageJob.setReducerClass(AverageReputationReducer.class);

		averageJob.setOutputKeyClass(Text.class);
		averageJob.setOutputValueClass(DoubleWritable.class);

		averageJob.setInputFormatClass(TextInputFormat.class);

		TextInputFormat.addInputPath(averageJob, inputDir);

		averageJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(averageJob, outputDir);
		
		return averageJob;
		
	}
}
