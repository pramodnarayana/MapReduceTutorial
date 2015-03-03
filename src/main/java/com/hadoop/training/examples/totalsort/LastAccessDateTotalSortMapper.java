package com.hadoop.training.examples.totalsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LastAccessDateTotalSortMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		context.write(key, value);

	}

}
