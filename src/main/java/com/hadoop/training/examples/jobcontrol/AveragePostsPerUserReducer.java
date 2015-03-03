package com.hadoop.training.examples.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveragePostsPerUserReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	public static final String AVERAGE_CALC_GROUP = "AverageCalculation"; //Counter group name
	public static final String USERS_COUNTER_NAME = "userCount"; // Counter name
	private LongWritable outValue = new LongWritable();
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		// for each unique userId (key), reduce function is called. Hence incrementing the counter will give total number of unique users 
		context.getCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME).increment(1);
		
		// Summarization Pattern, for each user number of posts is calculated
		for(LongWritable val : values) {
			sum += val.get();
		}
		
		outValue.set(sum);
		context.write(key, outValue);
	}
}
