package com.xiao.warning;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class WarningHandler extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.jar", "student-0.0.1-SNAPSHOT.jar");
		Job warning = Job.getInstance(conf);
		// warning.setJarByClass(WarningHandler.class);

		String basePath = "hdfs://hadoop:9000/";
		Path mapper1InputPath = new Path(basePath + "xyyj/input/yjjg/tmp/2014-2015-2");
		Path mapper2InputPath = new Path(basePath + "xyyj/input/yjjg/history");
		Path outPath = new Path(basePath + "xyyj/output/warning/2014-2015-2");

		// String basePath = "hdfs://master:9000/";
		// Path cjbInputPath = new Path(basePath + args[0]);
		// Path kcInputPath = new Path(basePath + args[1]);
		// Path outPath = new Path(basePath + args[2]);
		MultipleInputs.addInputPath(warning, mapper1InputPath, TextInputFormat.class, WarningMapper1.class);
		MultipleInputs.addInputPath(warning, mapper2InputPath, TextInputFormat.class, WarningMapper2.class);
		FileOutputFormat.setOutputPath(warning, outPath);

		// 根据学号分区
		warning.setPartitionerClass(FirstPartitioner.class);
		// 根据学号顺序，学年逆序，学期逆序，type顺序
		warning.setSortComparatorClass(KeyComparator.class);
		// 根据学号分组
		warning.setGroupingComparatorClass(GroupComparator.class);

		warning.setOutputKeyClass(Text.class);
		warning.setOutputValueClass(Text.class);

		warning.setMapOutputKeyClass(CombinationKey2.class);
		warning.setMapOutputValueClass(Text.class);

		warning.setReducerClass(JoinWarningReducer.class);

		return warning.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WarningHandler(), args);
		System.exit(exitCode);
	}

	// 上学期成绩数据
	static class WarningMapper1 extends Mapper<LongWritable, Text, CombinationKey2, Text> {
		private WarningParser1 parser = new WarningParser1();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(new CombinationKey2(parser.getXH(), parser.getXN(), parser.getXQ(), "0"),
						new Text("0" + "##########" + value.toString()));
			}
		}
	}

	// 成绩历史数据
	static class WarningMapper2 extends Mapper<LongWritable, Text, CombinationKey2, Text> {
		private WarningParser2 parser = new WarningParser2();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(new CombinationKey2(parser.getXH(), parser.getXN(), parser.getXQ(), "1"),
						new Text("1" + "##########" + value.toString()));
			}
		}
	}

	static class JoinWarningReducer extends Reducer<CombinationKey2, Text, Text, Text> {
		private WarningParser1 parser1 = new WarningParser1();
		private WarningParser2 parser2 = new WarningParser2();

		@Override
		protected void reduce(CombinationKey2 combinationKey, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Iterator<Text> iter = value.iterator();
			StringBuffer last = new StringBuffer();
			StringBuffer history = new StringBuffer();
			Map<String, String> map = new HashMap<String, String>();
			int yjjb = -1;
			String xn = "";
			int xq = 0;
			String xh = "";
			boolean flag = false;
			//最终级别
			int zzjb = 0;
			while (iter.hasNext()) {
				Text record = iter.next();
				parser1.parse(record);
				if ("0".equals(parser1.getType())) {
					// 统计上一学期级别
					xn = parser1.getXN();
					xq = Integer.parseInt(parser1.getXQ());
					xh = parser1.getXH();
					yjjb = Integer.parseInt(parser1.getYJJB());
					last.append(parser1.getYJYY());
					flag = true;
					zzjb = yjjb;
				} else {
					// 统计历史级别
					parser2.parse(record);
					history.append(parser2.getYJYY());
					map.put(parser2.getXN().trim() + "-" + parser2.getXQ().trim(), parser2.getYJJB());
				}
			}
			if (flag) {
				int year = Integer.parseInt(xn.split("-")[0]);
				StringBuffer content = new StringBuffer();
				switch (yjjb) {
				case 1:
					if (xq == 1) {
						if (map.get((year - 1) + "-" + (year) + "-2") != null
								&& map.get((year - 1) + "-" + (year) + "-2") == "1") {

						} else {
							break;
						}
						if (map.get((year - 1) + "-" + (year) + "-1") != null
								&& map.get((year - 1) + "-" + (year) + "-1") == "1") {
							zzjb = 4;
							content.append("退学警示;");
						} else {
							break;
						}
						if (map.get((year - 2) + "-" + (year - 1) + "-2") != null
								&& map.get((year - 2) + "-" + (year - 1) + "-2") == "1") {
							zzjb = 5;
							content.replace(0, content.toString().length() - 5, content.toString());
							content.append("退学处理;");
						} else {
							break;
						}
					} else if (xq == 2) {
						if (map.get(year + "-" + (year + 1) + "-1") != null
								&& map.get(year + "-" + (year + 1) + "-1") == "1") {
						} else {
							break;
						}
						if (map.get((year - 1) + "-" + (year) + "-2") != null
								&& map.get((year - 1) + "-" + (year) + "-2") == "1") {
							zzjb = 4;
							content.append("退学警示;");
						} else {
							break;
						}
						if (map.get((year - 1) + "-" + year + "-1") != null
								&& map.get((year - 1) + "-" + year + "-1") == "1") {
							zzjb = 5;
							content.replace(0, content.toString().length() - 5, content.toString());
							content.append("退学处理;");
						} else {
							break;
						}
					}
					break;
				case 2:
					if (xq == 1) {
						if (map.get((year - 1) + "-" + (year) + "-2") != null
								&& (map.get((year - 1) + "-" + (year) + "-2") == "2"
										|| map.get((year - 1) + "-" + (year) + "-2") == "1")) {
							zzjb = 1;
							content.append("上次二级或一级警示;");
						} else {
							break;
						}
					} else if (xq == 2) {
						if (map.get(year + "-" + (year + 1) + "-1") != null
								&& (map.get(year + "-" + (year + 1) + "-1") == "2"
										|| map.get(year + "-" + (year + 1) + "-1") == "1")) {
							zzjb = 1;
							content.append("上次二级或一级警示;");
						} else {
							break;
						}
					}
					break;
				case 3:
					if (xq == 1) {
						if (map.get((year - 1) + "-" + (year) + "-2") != null
								&& (map.get((year - 1) + "-" + (year) + "-2") == "2"
										|| map.get((year - 1) + "-" + (year) + "-2") == "3")) {
							zzjb = 2;
							content.append("上次三级或二级警示;");
						} else {
							break;
						}
					} else if (xq == 2) {
						if (map.get(year + "-" + (year + 1) + "-1") != null
								&& ("2".equals(map.get(year + "-" + (year + 1) + "-1").trim())
										|| "3".equals(map.get(year + "-" + (year + 1) + "-1")))) {
							zzjb = 2;
							content.append("上次三级或二级警示;");
						} else {
							break;
						}
					}
					break;
				default:
					break;
				}
				if (zzjb != 0) {
					context.write(new Text(xn + "#,#" + xq + "#,#" + xh + "#,#"),
							new Text(zzjb + "#,#" + last.toString() + "#,#" + content.toString()));
				}
			}
		}
	}

	static class JoinWarningReducer2 extends Reducer<CombinationKey2, Text, Text, Text> {
		private WarningParser1 parser1 = new WarningParser1();
		private WarningParser2 parser2 = new WarningParser2();

		@Override
		protected void reduce(CombinationKey2 combinationKey, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			context.write(new Text("######################################"),
					new Text("######################################"));
			Iterator<Text> iter = value.iterator();
			while (iter.hasNext()) {
				Text record = iter.next();
				parser1.parse(record);
				if ("0".equals(parser1.getType())) {
					// 统计上一学期级别
					context.write(new Text(parser1.getXH() + "#,#" + parser1.getXN() + "#,#" + parser1.getXQ() + "#,#"
							+ parser1.getYJJB() + "#,#" + parser1.getYJYY()), new Text("parser1"));
				} else {
					// 统计历史级别
					parser2.parse(record);
					context.write(new Text(parser2.getXH() + "#,#" + parser2.getXN() + "#,#" + parser2.getXQ() + "#,#"
							+ parser2.getYJJB() + "#,#" + parser2.getYJYY()), new Text("parser2"));
				}
			}
			context.write(new Text("######################################"),
					new Text("######################################"));
		}
	}

	// 分区
	static class FirstPartitioner extends Partitioner<CombinationKey2, Text> {
		@Override
		public int getPartition(CombinationKey2 key, Text value, int numPartitioners) {
			// TODO Auto-generated method stub
			// 根据学号分区
			return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitioners;
		}

	}

	// 辅助排序
	static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(CombinationKey2.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			// 根据学号顺序，学年逆序，学期逆序，type顺序
			CombinationKey2 key1 = (CombinationKey2) a;
			CombinationKey2 key2 = (CombinationKey2) b;
			if (key1.getFirstKey().compareTo(key2.getFirstKey()) != 0) {
				return key1.getFirstKey().compareTo(key2.getFirstKey());
			} else if (key1.getFourKey().compareTo(key2.getFourKey()) != 0) {
				return key1.getFourKey().compareTo(key2.getFourKey());
			} else if (key1.getSecondKey().compareTo(key2.getSecondKey()) != 0) {
				return -key1.getSecondKey().compareTo(key2.getSecondKey());
			} else {
				return -key1.getThirdKey().compareTo(key2.getThirdKey());
			}
		}
	}

	// 分组
	static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(CombinationKey2.class, true);
		}

		// 根据学号分组
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			CombinationKey2 key1 = (CombinationKey2) a;
			CombinationKey2 key2 = (CombinationKey2) b;
			if (key1.getFirstKey().compareTo(key2.getFirstKey()) != 0) {
				return key1.getFirstKey().compareTo(key2.getFirstKey());
			} else {
				return 0;
			}
		}
	}
}
