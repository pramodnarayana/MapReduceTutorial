package com.hadoop.training.examples.reducejoin;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.training.examples.utils.MRDPUtils;

public class UserMapper extends Mapper<LongWritable, Text, Text, Text>{

	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

		String userId = parsed.get("Id");

		if (userId == null) {
			return;
		}

		// The foreign join key is the user ID
		outkey.set(userId);

		// Flag this record for the reducer and then output
		outvalue.set("A" + value.toString());
		context.write(outkey, outvalue);
	}
}
