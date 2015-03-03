package com.hadoop.training.examples.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReputationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	private DoubleWritable outValue = new DoubleWritable();
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		double sum = 0.0;
        long count = 0;				
		for(DoubleWritable val : values) {
			sum += val.get();
			count++;
		}
		outValue.set(sum/count);
		context.write(key, outValue);
	}
}
