package com.xiao.warning;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.Text;

import com.xiaox.util.DataParser;

public class WarningParser2 extends DataParser {

	private String XN;
	private String XQ;
	private String XH;
	// 预警级别
	private String YJJB;
	// 预警原因
	private String YJYY;

	private static Properties pps;
	static {
		pps = new Properties();
		try {
			pps.load(new FileInputStream("src/main/java/properities/args.properities"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void parse(String record) {
		// 2012-2013#,#1#,#010611125#,# 3#,#1门课程不及格;
		String[] data = record.split("#,#");
		try {
			this.setXN(data[0].trim());
			this.setXQ(data[1].trim());
			this.setXH(data[2].trim());
			this.setYJJB(data[3].trim());
			this.setYJYY(data[4].trim());
		} catch (Exception e) {
			System.out.println("++++++++++++++++++++++++++WarningParser++++++++++++++++++++++++++");
			e.printStackTrace();
			System.out.println();
			System.out.println(record);
			System.out.println("++++++++++++++++++++++++++WarningParser++++++++++++++++++++++++++");
			System.out.println();
		}
	}

	// 过滤非给定学年学期
	public boolean isValidData() {
		String xn = pps.getProperty("lastTerm_XN").trim();
		String xq = pps.getProperty("lastTerm_XQ").trim();
		int year1 = Integer.parseInt(xn.split("-")[0]);
		int year2 = Integer.parseInt(this.getXN().split("-")[0]);
		// 学年学期
		if ("1".equals(xq)) {
			if (year1 > year2) {
				return true;
			} else {
				return false;
			}
		} else if ("2".equals(xq)) {
			if (year1 == year2 && "1".equals(this.getXQ().trim())) {
				return true;
			} else if (year1 > year2) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public void parse(Text record) {
		String[] data = record.toString().split("##########");
		if (data.length > 1) {
			super.setType(data[0].trim());
			if ("0".equals(data[0].trim())) {
				return;
			} else {
				parse(data[1]);
			}
		} else {
			parse(record.toString());
		}
	}

	public String getXN() {
		return XN;
	}

	public void setXN(String xN) {
		XN = xN;
	}

	public String getXQ() {
		return XQ;
	}

	public void setXQ(String xQ) {
		XQ = xQ;
	}

	public String getXH() {
		return XH;
	}

	public void setXH(String xH) {
		XH = xH;
	}

	public String getYJJB() {
		return YJJB;
	}

	public void setYJJB(String yJJB) {
		YJJB = yJJB;
	}

	public String getYJYY() {
		return YJYY;
	}

	public void setYJYY(String yJYY) {
		YJYY = yJYY;
	}

}
