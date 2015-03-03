package com.hadoop.training.examples.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageReputationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	private static final Text GROUP_ALL_KEY = new Text("Average Reputation");
	private DoubleWritable outValue = new DoubleWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split("\t");
		String strReputation = tokens[2];
		if (strReputation != null && !"".equals(strReputation) && !"null".equals(strReputation)) {
			try {
				double reputation = Double.parseDouble(strReputation);
				outValue.set(reputation);
				context.write(GROUP_ALL_KEY, outValue);
			}
			catch (NumberFormatException e) {
				System.out.println("This is not a number");
				System.out.println(e.getMessage());
			}
		}
	}
}
