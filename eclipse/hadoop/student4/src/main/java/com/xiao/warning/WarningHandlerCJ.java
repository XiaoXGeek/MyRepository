package com.xiao.warning;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WarningHandlerCJ extends Configured implements Tool {

	private static Properties pps;
	public static String XN;
	public static String XQ;
	static {
		pps = new Properties();
		try {
			pps.load(new FileInputStream("src/main/java/properities/args.properities"));
			WarningHandlerCJ.XN = pps.getProperty("lastTerm_XN");
			WarningHandlerCJ.XQ = pps.getProperty("lastTerm_XQ");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {

		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.jar", "student-0.0.1-SNAPSHOT.jar");
		Job warning = Job.getInstance(conf);
		// warning.setJarByClass(WarningHandler.class);

		String basePath = "hdfs://hadoop:9000/";
		Path mapper1InputPath = new Path(basePath + "xyyj/input/apre");
		Path mapper2InputPath = new Path(basePath + "xyyj/input/score/");
		Path outPath = new Path(basePath + "xyyj/output/warning/2016-2017-2");

		// String basePath = "hdfs://master:9000/";
		// Path cjbInputPath = new Path(basePath + args[0]);
		// Path kcInputPath = new Path(basePath + args[1]);
		// Path outPath = new Path(basePath + args[2]);
		MultipleInputs.addInputPath(warning, mapper1InputPath, TextInputFormat.class, WarningMapper1.class);
		MultipleInputs.addInputPath(warning, mapper2InputPath, TextInputFormat.class, WarningMapper2.class);
		FileOutputFormat.setOutputPath(warning, outPath);

		// 根据学号分区
		warning.setPartitionerClass(FirstPartitioner.class);
		// //根据学号顺序，学年逆序，学期逆序，type顺序
		// warning.setSortComparatorClass(KeyComparator.class);
		// 根据学号分组
		warning.setGroupingComparatorClass(GroupComparator.class);

		warning.setOutputKeyClass(Text.class);
		warning.setOutputValueClass(Text.class);

		warning.setMapOutputKeyClass(CombinationKey.class);
		warning.setMapOutputValueClass(Text.class);

		warning.setReducerClass(JoinWarningReducer.class);

		return warning.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WarningHandlerCJ(), args);
		System.exit(exitCode);
	}

	// 警示条件1
	static class WarningMapper1 extends Mapper<LongWritable, Text, CombinationKey, Text> {
		private CJBAndXSBParser parser = new CJBAndXSBParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(new CombinationKey(parser.getXH(), "0"), new Text("0" + "##########" + value.toString()));
			}
		}
	}

	// 警示条件2，3
	static class WarningMapper2 extends Mapper<LongWritable, Text, CombinationKey, Text> {
		private CreditParser creditParser = new CreditParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			creditParser.parse(value);
			if (creditParser.isValidData()) {
				context.write(new CombinationKey(creditParser.getXH(), "1"),
						new Text("1" + "##########" + value.toString()));
			}
		}
	}

	static class JoinWarningReducer extends Reducer<CombinationKey, Text, Text, Text> {
		private CJBAndXSBParser cxParser = new CJBAndXSBParser();
		private CreditParser creditParser = new CreditParser();

		@Override
		protected void reduce(CombinationKey combinationKey, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Iterator<Text> iter = value.iterator();
			//上一学期不及格课程
			int l_kc = 0;
			//累积不及格课程
			int a_kc = 0;
			//累积不及格学分
			float a_xf = 0;
			while (iter.hasNext()) {
				Text record = iter.next();
				cxParser.parse(record);
				if ("0".equals(cxParser.getType())) {
					// 统计上一学期不及格课程数
					if (!"0".equals(cxParser.getBKCJ())) {
						float score = Float.parseFloat(cxParser.getBKCJ());
						if (score < 60) {
							l_kc++;
						}
					}
				} else {
					// 统计累计不及格课程数和学分
					creditParser.parse(record);
					if (creditParser.getZSCJ().floatValue() < 60) {
						a_kc++;
						a_xf += Float.valueOf(creditParser.getXF());
					}
				}
			}
			
			//存储对应预警级别的原因
			StringBuffer reason = new StringBuffer();
			
			//上学期不及格课程数目对应预警级别
			switch (l_kc) {
			case 0:
				break;
			case 1:
				reason.append("3" + "#" + l_kc + "门课程不及格" + ";");
				break;
			case 2:
				reason.append("2" + "#" + l_kc + "门课程不及格" + ";");
				break;
			case 3:
				reason.append("1" + "#" + l_kc + "门课程不及格" + ";");
				break;
			default:
				reason.append("1" + "#" + l_kc + "门课程不及格" + ";");
				break;
			}
			
			//累积不及格课程数目对应预警级别
			if (a_kc >= 2 && a_kc < 5) {
				reason.append("3" + "#累计" + a_kc + "门课程不及格" + ";");
			} else if (a_kc >= 5 && a_kc < 8) {
				reason.append("2" + "#累计" + a_kc + "门课程不及格" + ";");
			} else if (a_kc >= 8) {
				reason.append("1" + "#累计" + a_kc + "门课程不及格" + ";");
			}

			//累积不及格学分对应预警级别
			if (a_xf >= 15 && a_xf < 24) {
				reason.append("2" + "#累计有" + a_xf + "个不及格学分" + ";");
			} else if (a_xf >= 24) {
				reason.append("1" + "#累计有" + a_xf + "个不及格学分" + ";");
			}

			StringBuffer str = new StringBuffer();
			if (!"".equals(reason.toString())) {
				String[] data = reason.toString().substring(0, reason.toString().length() - 1).split(";");
				int min = 4;
				//遍历预警级别，找到最高的预警级别
				for (int i = 0; i < data.length; i++) {
					if (Integer.parseInt(data[i].split("#")[0]) <= min) {
						min = Integer.parseInt(data[i].split("#")[0]);
					}
					str.append(data[i].split("#")[1] + ";");
				}
				context.write(new Text(WarningHandlerCJ.XN + "#,#" + WarningHandlerCJ.XQ + "#,#"
						+ combinationKey.getFirstKey() + "#,#"), new Text(min + "#,#" + str.toString()));
			}
		}
	}

	// 分区
	static class FirstPartitioner extends Partitioner<CombinationKey, Text> {
		@Override
		public int getPartition(CombinationKey key, Text value, int numPartitioners) {
			// TODO Auto-generated method stub
			// 根据学号分区
			return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitioners;
		}

	}

	// 分组
	static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(CombinationKey.class, true);
		}

		// 根据学号分组
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			CombinationKey key1 = (CombinationKey) a;
			CombinationKey key2 = (CombinationKey) b;
			if (key1.getFirstKey().compareTo(key2.getFirstKey()) != 0) {
				return key1.getFirstKey().compareTo(key2.getFirstKey());
			} else {
				return 0;
			}
		}
	}
}
