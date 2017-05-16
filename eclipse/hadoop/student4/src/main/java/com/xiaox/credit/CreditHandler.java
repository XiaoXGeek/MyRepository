package com.xiaox.credit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 
 * @author xiaox
 *  基础版本的credit计算
 *
 */
public class CreditHandler extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		Job credit = Job.getInstance(conf);

		credit.setJarByClass(CreditHandler.class);

		credit.setOutputKeyClass(Text.class);
		credit.setOutputValueClass(Text.class);

		credit.setMapOutputKeyClass(CombinationKey.class);
		credit.setMapOutputValueClass(Text.class);

		credit.setMapperClass(CreditMapper.class);
		credit.setReducerClass(CreditReducer.class);
		// wcjob.setReducerClass(CreditMultiReduce.class);
		String basePath = "hdfs://hadoop01:9000/";
		FileInputFormat.setInputPaths(credit, new Path(basePath + "cslg/input/score"));
		FileOutputFormat.setOutputPath(credit, new Path(basePath + "cslg/output/credit"));

		return credit.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CreditHandler(), args);
		System.exit(exitCode);
	}

	static class CreditMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
		private CreditParser parser = new CreditParser();
		private CombinationKey combinationKey = new CombinationKey();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			combinationKey.setFirstKey(new Text(parser.getXN()));
			combinationKey.setSecondKey(new Text(String.valueOf(parser.getXQ())));
			combinationKey.setThirdKey(new Text(parser.getXH()));
			if (parser.isValidData()) {
				context.write(combinationKey, value);
			}
		}
	}

	static class CreditReducer extends Reducer<CombinationKey, Text, Text, Text> {
		private CreditParser parser = new CreditParser();

		@Override
		protected void reduce(CombinationKey combinationKey, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			float credit = 0;
			for (Text record : value) {
				parser.parse(record);
				if (parser.getZSCJ().floatValue() >= 60) {
					credit = credit + Float.valueOf(parser.getXF());
				}

			}
			context.write(new Text(combinationKey.toString()),
					new Text(parser.getXM() + "#,#" + String.valueOf(credit)));
		}
	}

	static class CreditMultiReduce extends Reducer<CombinationKey, Text, Text, Text> {
		private CreditParser parser = new CreditParser();
		private MultipleOutputs<Text, Text> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(CombinationKey combinationKey, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			float credit = 0;
			for (Text record : value) {
				parser.parse(record);
				credit = credit + Float.valueOf(parser.getXF());
			}
			String baseOutputPath = String.format("%s/%s/%s/part", parser.getZY(), parser.getNJ(), parser.getBJ());
			multipleOutputs.write(new Text(combinationKey.toString()),
					new Text(parser.getXM() + "#,#" + String.valueOf(credit)), baseOutputPath);
		}

		@Override
		protected void cleanup(Reducer<CombinationKey, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs.close();
		}
	}

}
