package com.hadoop.training.examples.top10;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.training.examples.utils.MRDPUtils;

public class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text>{

	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
	
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	 
		for (Text value : values) {
			
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String strReputation = parsed.get("Reputation");
			repToRecordMap.put(Integer.parseInt(strReputation), new Text(value));
			
			if(repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		
		for (Text t : repToRecordMap.descendingMap().values()) {
			context.write(NullWritable.get(), t);
		}
	}
}
