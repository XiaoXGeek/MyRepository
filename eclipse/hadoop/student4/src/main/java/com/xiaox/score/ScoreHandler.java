package com.xiaox.score;

import java.io.IOException;
import java.math.BigDecimal;

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

public class ScoreHandler extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.jar", "student-0.0.1-SNAPSHOT.jar");
		Job score = Job.getInstance(conf);

		score.setJarByClass(ScoreHandler.class);

		score.setOutputKeyClass(Text.class);
		score.setOutputValueClass(Text.class);

		score.setMapOutputKeyClass(CombinationKey.class);
		score.setMapOutputValueClass(Text.class);

		score.setMapperClass(ScoreMapper.class);
		score.setReducerClass(ScoreReducer.class);
		// wcjob.setReducerClass(ScoreMultiReduce.class);

		String basePath = "hdfs://hadoop:9000/";
		FileInputFormat.setInputPaths(score, new Path(basePath + "xyyj/input/apre"));
		FileOutputFormat.setOutputPath(score, new Path(basePath + "xyyj/output/score/score_2"));

		return score.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ScoreHandler(), args);
		System.exit(exitCode);
	}

	static class ScoreMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
		private ScoreParser parser = new ScoreParser();
		private CombinationKey combinationKey = new CombinationKey();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			combinationKey.setFirstKey(new Text(parser.getXH()));
			combinationKey.setSecondKey(new Text(parser.getKCDM()));
			if (parser.isValidData()) {
				context.write(combinationKey, value);
			}
		}
	}

	static class ScoreReducer extends Reducer<CombinationKey, Text, Text, Text> {
		private ScoreParser parser = new ScoreParser();
		// private Properties pps = new Properties();

		@Override
		protected void reduce(CombinationKey combinationKey, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// pps.load(new
			// FileInputStream("src/main/java/properities/score.properities"));

			BigDecimal score = new BigDecimal(0);
			int bkCounter = 0;
			int cxCounter = 0;
			String xn = "";
			String xq = "";
			for (Text record : value) {
				parser.parse(record);
				if (!"0".equals(parser.getBKCJ())) {
					bkCounter++;
					// bao liu liang wei xiao shu,si she wu ru
					if (Float.valueOf(parser.getBKCJ()) > score.floatValue()) {
						score = new BigDecimal(parser.getBKCJ().trim());
						xn = parser.getXN();
						xq = parser.getXQ().toString();
					}
				}
				try {
					// cxbj != 0
					if ("1".equals(parser.getCXBJ().toString())) {
						cxCounter++;
					}
				} catch (Exception e) {
					// TODO: handle exception
					System.out.println("#################CXBJ####################");
					e.printStackTrace();
					System.out.println();
					System.out.println(parser.toString());
					System.out.println("#################CXBJ####################");
				}
				// cxcj != 0
				if (!"0".equals(parser.getCXCJ())) {
					cxCounter++;
					if (Float.valueOf(parser.getCXCJ()) > score.floatValue()) {
						score = new BigDecimal(parser.getCXCJ());
						xn = parser.getXN();
						xq = parser.getXQ().toString();
					}
				}
				if (parser.getZSCJ().floatValue() > score.floatValue()) {
					score = parser.getZSCJ();
					xn = parser.getXN();
					xq = parser.getXQ().toString();
				}
			}
			xn = "".equals(xn) ? parser.getXN() : xn;
			xq = "".equals(xq) ? parser.getXQ().toString() : xq;
			// ji suan ji dian
			float jdf = ((score.floatValue() - 60) / 10 + 1) >= 0 ? ((score.floatValue() - 60) / 10 + 1) : 0;
			String jd = String.format("%.1f", jdf);
			context.write(new Text(combinationKey.toString()),
					new Text(parser.getKCMC().trim() + "#,#" + xn + "#,#" + xq + "#,#" + parser.getXM().trim() + "#,#"
							+ score + "#,#" + parser.getXF() + "#,#" + bkCounter + "#,#" + cxCounter + "#,#" + jd
							+ "#,#" + parser.getDQSZJ() + "#,#" + parser.getSFXWK() + "#,#" + parser.getKCXZ()));
		}
	}

	static class ScoreMultiReduce extends Reducer<CombinationKey, Text, Text, Text> {
		private ScoreParser parser = new ScoreParser();
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
			BigDecimal score = new BigDecimal(0);
			int bkCounter = 0;
			int cxCounter = 0;
			Text maxText = new Text();
			for (Text record : value) {
				parser.parse(record);
				maxText = record;
				if (!"null".equals(parser.getBKCJ())) {
					bkCounter++;
					// bao liu liang wei xiao shu,si she wu ru
					if (Float.valueOf(parser.getBKCJ()) > score.floatValue()) {
						score = new BigDecimal(parser.getBKCJ());
						maxText = record;
					}
				}
				if (parser.getCXBJ().intValue() == 1) {
					cxCounter++;
					if (Float.valueOf(parser.getCXCJ()) > score.floatValue()) {
						score = new BigDecimal(parser.getCXCJ());
						maxText = record;
					}
				}
				if (parser.getZSCJ().floatValue() > score.floatValue()) {
					score = parser.getZSCJ();
					maxText = record;
				}
			}
			parser.parse(maxText);
			String baseOutputPath = String.format("%s/%s/%s/part", parser.getZYDM(), parser.getNJ(), parser.getBJ());
			multipleOutputs.write(new Text(combinationKey.toString()),
					new Text(parser.getKCMC().trim() + "#,#" + parser.getXN() + "#,#" + parser.getXQ() + "#,#"
							+ parser.getXM().trim() + "#,#" + score + "#,#" + parser.getXF() + "#,#" + bkCounter + "#,#"
							+ cxCounter),
					baseOutputPath);
		}

		@Override
		protected void cleanup(Reducer<CombinationKey, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs.close();
		}
	}

}
