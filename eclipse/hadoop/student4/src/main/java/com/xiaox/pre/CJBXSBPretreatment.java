package com.xiaox.pre;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import com.xiaox.pre.cjb.CJBParser;
import com.xiaox.pre.xsb.XSBParser;
import com.xiaox.util.CJBXSBCombinationKey;

public class CJBXSBPretreatment extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.jar", "student3 0.0.1-SNAPSHOT");
		Job cjbXsbPre = Job.getInstance(conf);

		// cjbXsbPre.setJarByClass(CJBPre.class);

		String basePath = "hdfs://hadoop:9000/";
		Path cjbInputPath = new Path(basePath + "xyyj/input/cjb");
		Path xsbInputPath = new Path(basePath + "xyyj/input/xsb");
		Path outPath = new Path(basePath + "xyyj/output/cjbxsb_3");

		// String basePath = "hdfs://master:9000/";
		// Path cjbInputPath = new Path(basePath + args[0]);
		// Path kcInputPath = new Path(basePath + args[1]);
		// Path outPath = new Path(basePath + args[2]);
		MultipleInputs.addInputPath(cjbXsbPre, cjbInputPath, TextInputFormat.class, CJBMapper.class);
		MultipleInputs.addInputPath(cjbXsbPre, xsbInputPath, TextInputFormat.class, XSBMapper.class);
		FileOutputFormat.setOutputPath(cjbXsbPre, outPath);

		cjbXsbPre.setPartitionerClass(KeyPartitioner.class);
		cjbXsbPre.setGroupingComparatorClass(GroupComparator.class);

		cjbXsbPre.setOutputKeyClass(NullWritable.class);
		cjbXsbPre.setOutputValueClass(Text.class);

		cjbXsbPre.setMapOutputKeyClass(CJBXSBCombinationKey.class);
		cjbXsbPre.setMapOutputValueClass(Text.class);

		cjbXsbPre.setReducerClass(JoinPreReducer.class);

		return cjbXsbPre.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CJBXSBPretreatment(), args);
		System.exit(exitCode);
	}

	static class CJBMapper extends Mapper<LongWritable, Text, CJBXSBCombinationKey, Text> {
		private CJBParser parser = new CJBParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(new CJBXSBCombinationKey(parser.getZY(), parser.getXH(), "1"),
						new Text("1" + "##########" + value.toString()));
			}
		}
	}

	static class XSBMapper extends Mapper<LongWritable, Text, CJBXSBCombinationKey, Text> {
		private XSBParser parser = new XSBParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(new CJBXSBCombinationKey(parser.getZYDM(), parser.getXH(), "0"),
						new Text("0" + "##########" + value.toString()));
			}
		}
	}

	static class JoinPreReducer extends Reducer<CJBXSBCombinationKey, Text, NullWritable, Text> {
		private CJBParser cjbParser = new CJBParser();
		private XSBParser xsbParser = new XSBParser();

		@Override
		protected void reduce(CJBXSBCombinationKey key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = value.iterator();
			Text student = iter.next();
			xsbParser.parse(student);
			if ("1".equals(xsbParser.getType())) {
				//输出不存在对应学生数据的成绩数据
				cjbParser.parse(student);
				Text outValue = new Text(cjbParser.getXN() + "#,#" + cjbParser.getXQ() + "#,#" + cjbParser.getXKKH()
				+ "#,#" + cjbParser.getXH() + "#,#" + cjbParser.getXM() + "#,#" + cjbParser.getKCMC()
				+ "#,#" + cjbParser.getXF() + "#,#" + cjbParser.getZSCJ() + "#,#" + cjbParser.getCXBJ()
				+ "#,#" + cjbParser.getBKCJ() + "#,#" + cjbParser.getCXCJ() + "#,#" + cjbParser.getKCXZ()
				+ "#,#" + cjbParser.getFXBJ() + "#,#" + xsbParser.getDQSZJ() + "#,#" + xsbParser.getZYFX());
				context.write(NullWritable.get(), outValue);
			}
			while (iter.hasNext()) {
				Text record = iter.next();
					cjbParser.parse(record);
					Text outValue = new Text(cjbParser.getXN() + "#,#" + cjbParser.getXQ() + "#,#" + cjbParser.getXKKH()
							+ "#,#" + cjbParser.getXH() + "#,#" + cjbParser.getXM() + "#,#" + cjbParser.getKCMC()
							+ "#,#" + cjbParser.getXF() + "#,#" + cjbParser.getZSCJ() + "#,#" + cjbParser.getCXBJ()
							+ "#,#" + cjbParser.getBKCJ() + "#,#" + cjbParser.getCXCJ() + "#,#" + cjbParser.getKCXZ()
							+ "#,#" + cjbParser.getFXBJ() + "#,#" + xsbParser.getDQSZJ() + "#,#" + xsbParser.getZYFX());
					context.write(NullWritable.get(), outValue);
			}
		}
	}

	static class KeyPartitioner extends Partitioner<CJBXSBCombinationKey, Text> {

		@Override
		public int getPartition(CJBXSBCombinationKey key, Text value, int numPartitioners) {
			// TODO Auto-generated method stub
			// 按照专业代码进行分区
			return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitioners;
		}

	}

	static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(CJBXSBCombinationKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			CJBXSBCombinationKey key1 = (CJBXSBCombinationKey) a;
			CJBXSBCombinationKey key2 = (CJBXSBCombinationKey) b;
			//按照专业代码和学号进行分组
			if (key1.getFirstKey().compareTo(key2.getFirstKey()) != 0) {
				return key1.getFirstKey().compareTo(key2.getFirstKey());
			} else if (key1.getSecondKey().compareTo(key2.getSecondKey()) != 0) {
				return key1.getSecondKey().compareTo(key2.getSecondKey());
			} else {
				return 0;
			}

		}
	}
}
