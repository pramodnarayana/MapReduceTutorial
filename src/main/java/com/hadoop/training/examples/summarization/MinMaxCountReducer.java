package com.hadoop.training.examples.summarization;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>{

	private MinMaxCountTuple result = new MinMaxCountTuple();
	
	public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
		result.setMin(null);
		result.setMax(null);
		int sum = 0;
		
		for(MinMaxCountTuple val : values) {
			
			if (result.getMin() == null || result.getMin().compareTo(val.getMin()) > 0) {
				result.setMin(val.getMin());
			}
			
			if(result.getMax() == null || result.getMax().compareTo(val.getMax()) < 0) {
				result.setMax(val.getMax());
			}
			
			sum += val.getCount();
		}
		result.setCount(sum);
		context.write(key, result);
	}

}
