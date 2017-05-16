package com.xiao.warning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CombinationKey2 implements WritableComparable<CombinationKey2> {
	private Text firstKey;
	private Text secondKey;
	private Text thirdKey;
	private Text fourKey;

	public CombinationKey2(String firstKey, String secondKey, String thirdKey, String fourKey) {
		this.firstKey = new Text(firstKey);
		this.secondKey = new Text(secondKey);
		this.thirdKey = new Text(thirdKey);
		this.fourKey = new Text(fourKey);
	}

	public CombinationKey2() {
		this.firstKey = new Text();
		this.secondKey = new Text();
		this.thirdKey = new Text();
		this.fourKey = new Text();
	}

	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		this.firstKey.readFields(dataInput);
		this.secondKey.readFields(dataInput);
		this.thirdKey.readFields(dataInput);
		this.fourKey.readFields(dataInput);
	}

	public void write(DataOutput outPut) throws IOException {
		// TODO Auto-generated method stub
		this.firstKey.write(outPut);
		this.secondKey.write(outPut);
		this.thirdKey.write(outPut);
		this.fourKey.write(outPut);
	}

	/**
	 * 自定义比较策略 注意：该比较策略用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，
	 * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
	 */
	public int compareTo(CombinationKey2 other) {
		// TODO Auto-generated method stub
		// logger.info("-------CombinationKey flag-------");
		// return this.secondKey.compareTo(combinationKey.getSecondKey());
		if (this.firstKey.compareTo(other.getFirstKey()) != 0) {
			return this.firstKey.compareTo(other.getFirstKey());
		} else if (this.secondKey.compareTo(other.secondKey) != 0) {
			return this.secondKey.compareTo(other.secondKey);
		} else if (this.thirdKey.compareTo(other.thirdKey) != 0) {
			return this.thirdKey.compareTo(other.thirdKey);
		} else {
			return this.fourKey.compareTo(other.fourKey);
		}
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.firstKey + "#,#" + this.secondKey + "#,#" + this.thirdKey + "#,#" + this.fourKey;
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

	public Text getThirdKey() {
		return thirdKey;
	}

	public void setThirdKey(Text thirdKey) {
		this.thirdKey = thirdKey;
	}

	public Text getFourKey() {
		return fourKey;
	}

	public void setFourKey(Text fourKey) {
		this.fourKey = fourKey;
	}

}
