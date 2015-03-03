package com.hadoop.training.examples.loganalysis;

/*
GeoWebMapper.java: mapper for GeoWeb application

Last Updated 10/17/13

Dave Jaffe, Dell Solution Centers

Distributed under Creative Commons with Attribution by Dave Jaffe (dave_jaffe@dell.com).  
Provided as-is without any warranties or conditions.

See documentation in GeoWeb.java driver program
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationHourlyClicksMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	HashMap<String,String> ip_location = new HashMap<String, String>();
	private final static IntWritable one = new IntWritable(1);
	private Text textObject = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		// Read ClassB IP addresses file from the Distributed Cache and create a HashMap
		// with Key as IP address and country Code as the value
		// Ex: 113.204 CN
		BufferedReader br = null;
		try {


			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String classb_line;
			
			if (files != null && files.length != 0) {

				for (Path p :  files) {
					br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(p.toString()))));
					while ((classb_line = br.readLine()) != null)
					{
						//System.out.println("classb line: " + classb_line);
						String[] fields = classb_line.toString().split(" ");
						if (fields.length == 2) ip_location.put(fields[0], fields[1]);
					}
				}
			}
		}
		catch (IOException e) {
			br.close();
			throw new RuntimeException(e);
			
		}	
		br.close();

	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// Processes Apache logs of form:
		// 39.44.210.129 - - [01/Jan/2012:00:11:14 -0500] "GET /ds2/dsbrowse.php?\
		// browsetype=category HTTP/1.1" 200 2147 "http://72.8.133.189/"\
		// "Mozilla/5.0 (Windows; U; Windows NT 5.1; ja; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3"

		String line = value.toString();
		String[] fields = line.split(" ");
		//if (fields.length <= 3) System.out.println(line);
		if (fields.length > 3)
		{
			String ip_address = fields[0];
			String time_stamp = fields[3];
			String[] octet = ip_address.split("\\.");
			if (octet.length > 1)
			{
				String ctry_code = ip_location.get(octet[0] + "." + octet[1]);
				if (ctry_code != null)
				{
					if (time_stamp.length() <15) {
						System.out.println("line= " + line + " time_stamp= " + time_stamp);return;
					}
					String hour = time_stamp.substring(13,15);
					if (hour != null)
					{
						textObject.set(ctry_code + hour);
						context.write(textObject, one);
					} // End if (hour != null)
				} // End if (ctry_code != null)
			} // End if (octet.length > 1)
		} // End if (fields.length > 3)
	} // End Map
} // End Class GeoWebMapper
