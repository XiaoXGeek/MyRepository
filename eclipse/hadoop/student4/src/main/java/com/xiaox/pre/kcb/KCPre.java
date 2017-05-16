package com.xiaox.pre.kcb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xiaox.pre.cjb.CJBPre;

public class KCPre extends Configured implements Tool{

	
	static class KCPreMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private KCParser parser = new KCParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(key, new Text(parser.toString()));
			}
		}
	}

	static class KCPreReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			for (Text record : value) {
				context.write(NullWritable.get(),record);
			}

		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.jar", "student-0.0.1-SNAPSHOT.jar");
		Job kc = Job.getInstance(conf);

		kc.setJarByClass(CJBPre.class);

		kc.setOutputKeyClass(NullWritable.class);
		kc.setOutputValueClass(Text.class);

		kc.setMapOutputKeyClass(LongWritable.class);
		kc.setMapOutputValueClass(Text.class);

		kc.setMapperClass(KCPreMapper.class);
		kc.setReducerClass(KCPreReducer.class);

		String basePath = "hdfs://hadoop01:9000/";
		FileInputFormat.setInputPaths(kc, basePath + "cslg/input/kcb_all");
		FileOutputFormat.setOutputPath(kc, new Path(basePath + "cslg/output/kcb_all_7"));

		return kc.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new KCPre(), args);
		System.exit(exitCode);
	}

}
