package com.hadoop.training.examples.totalsort;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastAccessDateTotalSortReducer extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long ts = key.get();
		Date d = new Date(ts);
		String date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(d);
		for (Text t : values) {
			context.write(new Text(date), t);
		}
	}
}

