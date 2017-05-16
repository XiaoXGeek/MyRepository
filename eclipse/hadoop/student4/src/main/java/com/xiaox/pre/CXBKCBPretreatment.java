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

import com.xiaox.pre.cjb.CJBAndXSBParser;
import com.xiaox.pre.cjb.CJBPre;
import com.xiaox.pre.kcb.KCParser;
import com.xiaox.util.CJBXSBKCBCombinationKey;

public class CXBKCBPretreatment extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.jar", "student3 0.0.1-SNAPSHOT");
		Job pre = Job.getInstance(conf);

		pre.setJarByClass(CJBPre.class);

		String basePath = "hdfs://hadoop:9000/";
		Path cjbxsbInputPath = new Path(basePath + "xyyj/input/cjbxsb");
		Path kcInputPath = new Path(basePath + "xyyj/input/kcb");
		Path outPath = new Path(basePath + "xyyj/output/apre_6");

		// String basePath = "hdfs://master:9000/";
		// Path cjbInputPath = new Path(basePath + args[0]);
		// Path kcInputPath = new Path(basePath + args[1]);
		// Path outPath = new Path(basePath + args[2]);
		MultipleInputs.addInputPath(pre, cjbxsbInputPath, TextInputFormat.class, CJBXSBMapper.class);
		MultipleInputs.addInputPath(pre, kcInputPath, TextInputFormat.class, KCMapper.class);
		FileOutputFormat.setOutputPath(pre, outPath);

		pre.setPartitionerClass(KeyPartitioner.class);
		pre.setGroupingComparatorClass(GroupComparator.class);

		pre.setOutputKeyClass(NullWritable.class);
		pre.setOutputValueClass(Text.class);

		pre.setMapOutputKeyClass(CJBXSBKCBCombinationKey.class);
		pre.setMapOutputValueClass(Text.class);

		pre.setReducerClass(JoinPreReducer.class);

		return pre.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CXBKCBPretreatment(), args);
		System.exit(exitCode);
	}

	static class CJBXSBMapper extends Mapper<LongWritable, Text, CJBXSBKCBCombinationKey, Text> {
		private CJBAndXSBParser parser = new CJBAndXSBParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(new CJBXSBKCBCombinationKey(parser.getZYDM(), parser.getKCDM(), parser.getNJ(),
						parser.getZYFX(), "1"), new Text("1" + "##########" + value.toString()));
			}
		}
	}

	static class KCMapper extends Mapper<LongWritable, Text, CJBXSBKCBCombinationKey, Text> {
		private KCParser parser = new KCParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidData()) {
				context.write(new CJBXSBKCBCombinationKey(parser.getZYDM(), parser.getKCDM(), parser.getNJ(),parser.getZYFX(), "0"),
						new Text("0" + "##########" + value.toString()));
			}
		}
	}

	static class JoinPreReducer extends Reducer<CJBXSBKCBCombinationKey, Text, NullWritable, Text> {
		private CJBAndXSBParser cjbParser = new CJBAndXSBParser();
		private KCParser kcParser = new KCParser();

		@Override
		protected void reduce(CJBXSBKCBCombinationKey key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = value.iterator();
			Text course = iter.next();
			kcParser.parse(course);
			if ("1".equals(kcParser.getType())) {
				//对应的课程不存在时的成绩数据
				cjbParser.parse(course);
				Text outValue = new Text(cjbParser.getXN() + "#,#" + cjbParser.getXQ() + "#,#" + cjbParser.getXKKH()
						+ "#,#" + cjbParser.getXH() + "#,#" + cjbParser.getXM() + "#,#" + cjbParser.getKCMC()
						+ "#,#" + cjbParser.getXF() + "#,#" + cjbParser.getZSCJ() + "#,#" + cjbParser.getCXBJ()
						+ "#,#" + cjbParser.getBKCJ() + "#,#" + cjbParser.getCXCJ() + "#,#" + cjbParser.getKCXZ()
						+ "#,#" + cjbParser.getFXBJ() + "#,#" + cjbParser.getDQSZJ() + "#,#" + cjbParser.getZYFX()
						+ "#,#" + ("null".equals(kcParser.getSFXWK()) ? "否" : kcParser.getSFXWK()));
				context.write(NullWritable.get(), outValue);
			}

			//context.write(new Text(key.toString()), new Text(kcParser.toString()));
			while (iter.hasNext()) {
				Text record = iter.next();
					cjbParser.parse(record);
					Text outValue = new Text(cjbParser.getXN() + "#,#" + cjbParser.getXQ() + "#,#" + cjbParser.getXKKH()
							+ "#,#" + cjbParser.getXH() + "#,#" + cjbParser.getXM() + "#,#" + cjbParser.getKCMC()
							+ "#,#" + cjbParser.getXF() + "#,#" + cjbParser.getZSCJ() + "#,#" + cjbParser.getCXBJ()
							+ "#,#" + cjbParser.getBKCJ() + "#,#" + cjbParser.getCXCJ() + "#,#" + cjbParser.getKCXZ()
							+ "#,#" + cjbParser.getFXBJ() + "#,#" + cjbParser.getDQSZJ() + "#,#" + cjbParser.getZYFX()
							+ "#,#" + ("null".equals(kcParser.getSFXWK()) ? "否" : kcParser.getSFXWK()));
					context.write(NullWritable.get(), outValue);
			}
		}
	}

	static class KeyPartitioner extends Partitioner<CJBXSBKCBCombinationKey, Text> {

		@Override
		public int getPartition(CJBXSBKCBCombinationKey key, Text value, int numPartitioners) {
			// TODO Auto-generated method stub
			return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitioners;
		}

	}

	static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(CJBXSBKCBCombinationKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			CJBXSBKCBCombinationKey key1 = (CJBXSBKCBCombinationKey) a;
			CJBXSBKCBCombinationKey key2 = (CJBXSBKCBCombinationKey) b;
			if (key1.getFirstKey().compareTo(key2.getFirstKey()) != 0) {
				return key1.getFirstKey().compareTo(key2.getFirstKey());
			} else if (key1.getSecondKey().compareTo(key2.getSecondKey()) != 0) {
				return key1.getSecondKey().compareTo(key2.getSecondKey());
			} else if (key1.getThirdKey().compareTo(key2.getThirdKey()) != 0) {
				return key1.getThirdKey().compareTo(key2.getThirdKey());
			} else if (key1.getFourKey().compareTo(key2.getFourKey()) != 0) {
				return key1.getFourKey().compareTo(key2.getFourKey());
			} else {
				return 0;
			}

		}
	}
}
