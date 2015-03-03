package com.hadoop.training.examples.top10;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.training.examples.utils.MRDPUtils;


public class TopTenMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
	
	public void map(LongWritable key, Text value, Context context) {
		Map<String, String> parsed =  MRDPUtils.transformXmlToMap(value.toString());
		
		if (parsed == null ) {
			return;
		}
		
		String strUserId = parsed.get("Id");
		String strReputation = parsed.get("Reputation");
		
		if (strUserId == null || strReputation == null) {
			
			return;
		}
		
		repToRecordMap.put(Integer.parseInt(strReputation), new Text(value));
		
		if (repToRecordMap.size() > 10) {
			
			repToRecordMap.remove(repToRecordMap.firstKey());
		}
		
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		for (Text t : repToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}
