package com.xiaox.credit;

import java.io.IOException;
import java.text.DecimalFormat;

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
 *	升级版本的credit计算
 */
public class CreditHandler2 extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		Job credit = Job.getInstance(conf);

		credit.setJarByClass(CreditHandler2.class);

		credit.setOutputKeyClass(Text.class);
		credit.setOutputValueClass(Text.class);

		credit.setMapOutputKeyClass(CombinationKey.class);
		credit.setMapOutputValueClass(Text.class);

		credit.setMapperClass(CreditMapper.class);
		credit.setReducerClass(CreditReducer.class);
		// wcjob.setReducerClass(CreditMultiReduce.class);
		String basePath = "hdfs://hadoop:9000/";
		FileInputFormat.setInputPaths(credit, new Path(basePath + "xyyj/input/score"));
		FileOutputFormat.setOutputPath(credit, new Path(basePath + "xyyj/output/credit/credit_3"));

		return credit.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CreditHandler2(), args);
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
		private DecimalFormat format = new DecimalFormat(".00");
		@Override
		protected void reduce(CombinationKey combinationKey, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//全部学分
			float  a_credit = 0;
			//获得的学分
			float g_credit = 0;
			//学位课全部学分
			float x_credit = 0;
			//学位课获得的学分
			float xg_credit=0;
			// 加权总绩点
			float a_jd_sum = 0;
			// 加权学位课总绩点
			float x_jd_sum = 0;
			for (Text record : value) {
				parser.parse(record);
				a_credit = a_credit + Float.valueOf(parser.getXF());
				if("是".equals(parser.getSFXWK())){
					x_credit += Float.valueOf(parser.getXF());
				}
				if (parser.getZSCJ().floatValue() >= 60) {
					g_credit = g_credit + Float.valueOf(parser.getXF());
					a_jd_sum += Float.valueOf(parser.getJD())*Float.valueOf(parser.getXF());
					if("是".equals(parser.getSFXWK())){
						xg_credit += Float.valueOf(parser.getXF());
						x_jd_sum += Float.valueOf(parser.getJD())*Float.valueOf(parser.getXF());
					}
				}
			}
			
			context.write(new Text(combinationKey.toString()),
					new Text(parser.getXM() + "#,#" + format.format(a_credit)+ "#,#" + format.format(g_credit)+ "#,#" + format.format(x_credit)+ "#,#" + format.format(xg_credit)+ "#,#" + format.format(a_jd_sum)+ "#,#" + format.format(x_jd_sum)));
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
