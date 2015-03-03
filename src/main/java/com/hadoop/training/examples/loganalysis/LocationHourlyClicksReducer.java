package com.hadoop.training.examples.loganalysis;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
SumReducer
 */ 
public class LocationHourlyClicksReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)	throws IOException, InterruptedException
	{
		int wordCount = 0;
		for (IntWritable value : values)
		{
			wordCount += value.get();
		}
		context.write(key, new IntWritable(wordCount));
	} 
}

