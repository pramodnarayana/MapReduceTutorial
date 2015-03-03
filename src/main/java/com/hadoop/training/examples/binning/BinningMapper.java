package com.hadoop.training.examples.binning;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.hadoop.training.examples.utils.MRDPUtils;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs = null;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void setup(Context context) {
		// Create a new MultipleOutputs using the context object
		multipleOutputs = new MultipleOutputs(context);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
				.toString());

		String rawtags = parsed.get("Tags");
		if (rawtags == null) {
			return;
		}

		// Tags are delimited by ><. i.e. <tag1><tag2><tag3>
		String[] tagTokens = StringEscapeUtils.unescapeHtml(rawtags).split(
				"><");

		// For each tag
		for (String tag : tagTokens) {
			// Remove any > or < from the token
			String groomed = tag.replaceAll(">|<", "").toLowerCase();

			// If this tag is one of the following, write to the named bin
			if (groomed.equalsIgnoreCase("hadoop")) {
				multipleOutputs.write("textbins", NullWritable.get(), value,  "hadoop/hadoop-tag");
				
			}

			if (groomed.equalsIgnoreCase("pig")) {
				multipleOutputs.write("textbins", NullWritable.get(), value,  "pig/pig-tag");
			}

			if (groomed.equalsIgnoreCase("hive")) {
				multipleOutputs.write("textbins", NullWritable.get(), value,  "hive/hive-tag");
			}

			if (groomed.equalsIgnoreCase("hbase")) {
				multipleOutputs.write("textbins", NullWritable.get(), value,  "hbase/hbase-tag");
			}
		}

		// Get the body of the post
		String post = parsed.get("Body");

		if (post == null) {
			return;
		}

		// If the post contains the word "hadoop", write it to its own bin
		if (post.toLowerCase().contains("hadoop")) {
			multipleOutputs.write("textbins", NullWritable.get(), value, "body/hadoop-post");
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		// Close multiple outputs!
		multipleOutputs.close();
	}
}
