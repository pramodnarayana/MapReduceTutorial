package com.hadoop.training.examples.jobcontrol;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.hadoop.training.examples.utils.MRDPUtils;


public class UserBinningMapper extends Mapper<Object, Text, Text, Text>{

	public static final String AVG_POSTS_PER_USER = "avg.posts.per.user";
	public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "aboveavg";
	public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "belowavg";

	public static void setAvgPostsPerUser(Job job, double avg) {
		job.getConfiguration().set(AVG_POSTS_PER_USER, Double.toString(avg));
	}

	public static double getAvgPostsPerUser(Configuration conf) {
		return Double.parseDouble(conf.get(AVG_POSTS_PER_USER));
	}

	private double average = 0.0;
	private MultipleOutputs<Text, Text> mos = null;
	private Text outKey = new Text();
	private Text outValue = new Text();
	private HashMap<String, String> userIdReputation = new HashMap<String, String>();


	/* This function will read the user dataset from Distributed cache. 
	 * And then java Map Data structure is created with userId as the Key and userReputation as the value
	 * 
	 */
	protected void setup(Context context) throws IOException {

		average =  getAvgPostsPerUser(context.getConfiguration());
		mos = new MultipleOutputs<Text, Text>(context);

		try {

			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

			if (files != null && files.length != 0) {

				for (Path p :  files) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(p.toString()))));
					String line = null;

					while((line = br.readLine()) != null) {

						Map<String, String> parsed = MRDPUtils.transformXmlToMap(line);
						String userId = parsed.get("Id");
						String reputation =  parsed.get("Reputation");
						if(userId != null && reputation != null) {
							userIdReputation.put(userId, reputation);
						}
					}
					if (br != null)	br.close();
				}
			}
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split("\t");
		String userId = tokens[0];
		int posts =  Integer.parseInt(tokens[1]);

		outKey.set(userId);
		outValue.set((long)posts + "\t" + userIdReputation.get(userId));

		if((double)posts < average) {
			mos.write(MULTIPLE_OUTPUTS_BELOW_NAME, outKey, outValue, MULTIPLE_OUTPUTS_BELOW_NAME + "/part" );
		}
		else {
			mos.write(MULTIPLE_OUTPUTS_ABOVE_NAME, outKey, outValue, MULTIPLE_OUTPUTS_ABOVE_NAME + "/part");
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		mos.close();
	}

}
