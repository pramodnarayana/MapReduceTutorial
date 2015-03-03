package com.hadoop.training.examples.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoinDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: ReduceSideJoin <user data> <comment data> <out> [inner|leftouter|rightouter|fullouter|anti]");
			System.exit(1);
		}

		String joinType = otherArgs[3];
		if (!(joinType.equalsIgnoreCase("inner")
				|| joinType.equalsIgnoreCase("leftouter")
				|| joinType.equalsIgnoreCase("rightouter")
				|| joinType.equalsIgnoreCase("fullouter") || joinType
					.equalsIgnoreCase("anti"))) {
			System.err
					.println("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
			System.exit(2);
		}

		Job job = new Job(conf, "Reduce Side Join");

		// Configure the join type
		job.getConfiguration().set("join.type", joinType);
		job.setJarByClass(ReduceSideJoinDriver.class);

		// Use multiple inputs to set which input uses what mapper
		// This will keep parsing of each data set separate from a logical
		// standpoint
		// However, this version of Hadoop has not upgraded MultipleInputs
		// to the mapreduce package, so we have to use the deprecated API.
		// Future releases have this in the "mapreduce" package.
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, UserMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(UserCommentJoinReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
