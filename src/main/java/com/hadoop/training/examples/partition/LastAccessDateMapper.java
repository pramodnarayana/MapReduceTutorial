package com.hadoop.training.examples.partition;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.training.examples.utils.MRDPUtils;

public class LastAccessDateMapper extends Mapper<Object, Text, IntWritable, Text>{

	// This object will format the date string into a Date object
    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private IntWritable outkey = new IntWritable();
    
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		String strdate = parsed.get("LastAccessDate");
		if(strdate == null) {
			return;
		}
		
		try {
			Date lastAccessDate = frmt.parse(strdate);
			Calendar calender =  Calendar.getInstance();
			calender.setTime(lastAccessDate);
			outkey.set(calender.get(Calendar.YEAR));
			context.write(outkey, value);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
}
