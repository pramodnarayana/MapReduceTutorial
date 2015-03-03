package com.hadoop.training.examples.jobchaining;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserIdSumReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
	public static final String USERS_COUNTER_NAME = "userCount";
	private LongWritable outValue = new LongWritable();
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		
		context.getCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME).increment(1);
		
		for(LongWritable val : values) {
			sum += val.get();
		}
		
		outValue.set(sum);
		context.write(key, outValue);
	}
}
