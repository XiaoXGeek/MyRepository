package com.xiaox.pre.cjb;

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

public class CJBPre extends Configured implements Tool {

	static class PreMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private CJBParser parser = new CJBParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(key, value);
			}
		}
	}

	static class PreReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		private CJBParser parser = new CJBParser();

		@Override
		protected void reduce(LongWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// 2014-2015,1,(2014-2015-1)-07200002-199000066-1,072413139,周雨,工程力学B,3.0,78,0,null,null,必修,0
			// TODO Auto-generated method stub
			//2012-2013,2,(2012-2013-2)-A0010060-201200030-1,072311115,刘夏,德语（4）,null,71.0,0,0,0
			for (Text record : value) {
				parser.parse(record);
				context.write(NullWritable.get(),
						new Text(parser.getXN() + "#,#" + parser.getXQ() + "#,#" + parser.getXKKH() + "#,#" + parser.getXH()
								+ "#,#" + parser.getXM() + "#,#" + parser.getKCMC() + "#,#" + parser.getXF() + "#,#"
								+ parser.getZSCJ() + "#,#" + parser.getCXBJ() + "#,#" + parser.getBKCJ() + "#,#"
								+ parser.getCXCJ()+"#,#"+parser.getKCXZ()+"#,#"+parser.getFXBJ()));
			}

		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.jar", "student-0.0.1-SNAPSHOT.jar");
		Job pre = Job.getInstance(conf);

		pre.setJarByClass(CJBPre.class);

		pre.setOutputKeyClass(LongWritable.class);
		pre.setOutputValueClass(Text.class);

		pre.setMapOutputKeyClass(LongWritable.class);
		pre.setMapOutputValueClass(Text.class);

		pre.setMapperClass(PreMapper.class);
		pre.setReducerClass(PreReducer.class);

		String basePath = "hdfs://hadoop01:9000/";
		FileInputFormat.setInputPaths(pre, basePath + "cslg/input/cjb_all");
		FileOutputFormat.setOutputPath(pre, new Path(basePath + "cslg/output/cjb/cjb_all_pre_1"));

		return pre.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CJBPre(), args);
		System.exit(exitCode);
	}

}
