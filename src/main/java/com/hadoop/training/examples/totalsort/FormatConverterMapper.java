package com.hadoop.training.examples.totalsort;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.training.examples.utils.MRDPUtils;

public class FormatConverterMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	private LongWritable outKey = new LongWritable();
	private final static SimpleDateFormat dateFrmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"); 

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		String accessDate = parsed.get("LastAccessDate");
		String userId = parsed.get("Id");
		String displayName = parsed.get("DisplayName");
		String strValue = userId + " " + displayName;
		if (accessDate != null) {

			try {
				outKey.set(dateFrmt.parse(accessDate).getTime());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}

			//outKey.set(accessDate);
			context.write(outKey, new Text(strValue));

		}
	}
}
