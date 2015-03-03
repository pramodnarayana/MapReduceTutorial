package com.hadoop.training.examples.summarization;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.training.examples.utils.MRDPUtils;

public class MinMaxCountMapper extends Mapper<LongWritable, Text, Text, MinMaxCountTuple>{

	private Text mapOutputKey = new Text();
	private MinMaxCountTuple mapOutputValue = new MinMaxCountTuple();
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		String strDate = parsed.get("CreationDate");
		String userId = parsed.get("UserId");
		
		if (strDate == null || userId == null ) {
			return;
		}
		
		Date creationDate;
		try {
			creationDate = frmt.parse(strDate);
		 	mapOutputValue.setMin(creationDate);
		    mapOutputValue.setMax(creationDate);
		    mapOutputValue.setCount(1);
			mapOutputKey.set(userId);
		}
		catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		context.write(mapOutputKey, mapOutputValue);
	}
}
