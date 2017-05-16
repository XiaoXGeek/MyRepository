package com.xiaox.credit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CombinationKey implements WritableComparable<CombinationKey> {
	// private static final Logger logger =
	// LoggerFactory.getLogger(CombinationKey.class);
	private Text firstKey;
	private Text secondKey;
	private Text thirdKey;

	public CombinationKey() {
		this.firstKey = new Text();
		this.secondKey = new Text();
		this.thirdKey = new Text();
	}

	public Text getFirstKey() {
		return this.firstKey;
	}

	public void setFirstKey(Text firstKey) {
		this.firstKey = firstKey;
	}

	public Text getSecondKey() {
		return this.secondKey;
	}

	public void setSecondKey(Text secondKey) {
		this.secondKey = secondKey;
	}

	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		this.firstKey.readFields(dataInput);
		this.secondKey.readFields(dataInput);
		this.thirdKey.readFields(dataInput);
	}

	public void write(DataOutput outPut) throws IOException {
		// TODO Auto-generated method stub
		this.firstKey.write(outPut);
		this.secondKey.write(outPut);
		this.thirdKey.write(outPut);
	}

	/**
	 * 自定义比较策略 注意：该比较策略用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，
	 * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
	 */
	public int compareTo(CombinationKey combinationKey) {
		// TODO Auto-generated method stub
		// logger.info("-------CombinationKey flag-------");
		// return this.secondKey.compareTo(combinationKey.getSecondKey());
		if (this.firstKey.compareTo(combinationKey.getFirstKey()) != 0) {
			return this.firstKey.compareTo(combinationKey.getFirstKey());
		} else if (this.secondKey.compareTo(combinationKey.secondKey) != 0) {
			return this.secondKey.compareTo(combinationKey.secondKey);
		} else {
			return this.thirdKey.compareTo(combinationKey.thirdKey);
		}
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.firstKey + "#,#" + this.secondKey + "#,#" + thirdKey + "#,#";
	}

	public Text getThirdKey() {
		return thirdKey;
	}

	public void setThirdKey(Text thirdKey) {
		this.thirdKey = thirdKey;
	}
}
