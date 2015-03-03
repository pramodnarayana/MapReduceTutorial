package com.hadoop.training.examples.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class LastAccessDatePartitioner extends Partitioner<IntWritable, Text> implements Configurable{

	private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
	private int minLastAccessDateYear = 0;
	private Configuration conf =  null;


	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return key.get() - this.minLastAccessDateYear;

	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
        this.conf = conf;
        minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
	}

	/**
	 * Sets the minimum possible last access date to subtract from each key
	 * to be partitioned<br>
	 * <br>
	 *
	 * That is, if the last min access date is "2008" and the key to
	 * partition is "2009", it will go to partition 2009 - 2008 = 1
	 *
	 * @param job
	 * The job to configure
	 * @param minLastAccessDateYear
	 * The minimum access date.
	 */

	public static void setMinLastAccessDateYear(Job job, int minLastAccessDateYear) {
		job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
	}

}
