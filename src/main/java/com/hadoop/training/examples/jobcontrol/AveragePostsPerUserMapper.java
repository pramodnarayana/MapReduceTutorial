package com.hadoop.training.examples.jobcontrol;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.training.examples.utils.MRDPUtils;

public class AveragePostsPerUserMapper extends Mapper<Object, Text, Text, LongWritable>{

	public static final String AVERAGE_CALC_GROUP = "AverageCalculation"; //Counter Group name
	public static final String POSTS_COUNTER_NAME = "postCount"; // Counter name
	private static final LongWritable ONE = new LongWritable(1);
	private Text outKey = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		String userId =  parsed.get("OwnerUserId");
		
		if (userId != null) {
			
			outKey.set(userId);
			//Summarization Pattern, Count number of posts per user.
			context.write(outKey, ONE);
			// For every post read, increment by 1. Hence total number of posts is calculated using built-in MapReduce counters
			context.getCounter(AVERAGE_CALC_GROUP, POSTS_COUNTER_NAME).increment(1); 
		}
	}
}
