package com.xiao.warning;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;

import org.apache.hadoop.io.Text;

import com.xiaox.util.DataParser;

public class CJBAndXSBParser extends DataParser {
	// //cjbParser.getXN() + "#,#" + cjbParser.getXQ() + "#,#" +
	// cjbParser.getXKKH()
	// + "#,#" + cjbParser.getXH() + "#,#" + cjbParser.getXM() + "#,#" +
	// cjbParser.getKCMC()
	// + "#,#" + cjbParser.getXF() + "#,#" + cjbParser.getZSCJ() + "#,#" +
	// cjbParser.getCXBJ()
	// + "#,#" + cjbParser.getBKCJ() + "#,#" + cjbParser.getCXCJ() + "#,#" +
	// cjbParser.getKCXZ()
	// + "#,#" + cjbParser.getFXBJ() + "#,#" + xsbParser.getDQSZJ() + "#,#" +
	// xsbParser.getZYFX());
	private String XN;
	private java.math.BigDecimal XQ;
	private String XKKH;
	private String XH;
	private String XM;
	private String KCMC;
	private String XF;
	private java.math.BigDecimal ZSCJ;
	private java.math.BigDecimal CXBJ;
	private String BKCJ;
	private String CXCJ;
	private String KCXZ;
	private String FXBJ;
	private String DQSZJ;
	private String ZYFX;
	private String ZYDM;
	private String KCDM;
	private String NJ;
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
		// 2012-2013#,#2#,#(2012-2013-2)-A0130054-199400007-1#,#010112101#,#金轶超#,#现代汉语（2）#,#3.0#,#83.0#,#0#,#0#,#
		// 0#,#null#,#0#,#2012#,#无方向
		// cjbParser.getXN() + "#,#" + cjbParser.getXQ() + "#,#" +
		// cjbParser.getXKKH()
		// "#,#" + cjbParser.getXH() + "#,#" + cjbParser.getXM() + "#,#" +
		// cjbParser.getKCMC()
		// "#,#" + cjbParser.getXF() + "#,#" + cjbParser.getZSCJ() + "#,#" +
		// cjbParser.getCXBJ()
		// "#,#" + cjbParser.getBKCJ() + "#,#" + cjbParser.getCXCJ() + "#,#" +
		// cjbParser.getKCXZ()85.0
		// "#,#" + cjbParser.getFXBJ() + "#,#" + xsbParser.getDQSZJ() + "#,#" +
		// xsbParser.getZYFX();

		// 2012-2013#,#2#,#(2012-2013-2)-P0050012-000000000-1#,#010108145#,#宋菡婷#,#学生体质健康标准#,#0#,#85.0#,#0#,#0#,#
		// 0#,#null#,#null#,#null#,#null
		try {
			String[] data = record.split("#,#");
			this.setXN(data[0].trim());
			this.setXQ(BigDecimal.valueOf(Integer.parseInt(data[1].trim())));
			if (data[2].trim().length() < 34) {
				String str = data[2].trim();
				// jian dan bu 0
				for (int i = data[2].trim().length(); i < 34; i++) {
					str += "0";
				}
				this.setXKKH(str);
				if (data[2].trim().length() >= 22) {
					this.setKCDM(data[2].trim().substring(14, 22));
				} else {
					this.setKCDM("00000000");
				}
			} else {
				this.setXKKH(data[2].trim());
				this.setKCDM(data[2].trim().substring(14, 22));
			}
			this.setXH(data[3].trim());
			this.setZYDM(data[3].trim().substring(0, data[3].trim().length() - 5));
			this.setNJ("20" + data[3].trim().substring(data[3].trim().length() - 5, data[3].trim().length() - 3));
			this.setXM(data[4].trim());
			this.setKCMC(data[5].trim());
			this.setXF(data[6].trim());
			this.setZSCJ(BigDecimal.valueOf(Double.parseDouble(data[7])));
			this.setCXBJ(BigDecimal.valueOf(Double.parseDouble(data[8])));
			this.setBKCJ(cxbkParse(data[9].trim()));
			this.setCXCJ(cxbkParse(data[10].trim()));
			this.setKCXZ(data[11].trim());
			this.setFXBJ(data[12].trim());
			this.setDQSZJ(data[13].trim());
			this.setZYFX(data[14].trim());
		} catch (Exception e) {
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++");
			e.printStackTrace();
			System.out.println();
			System.out.println(record);
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++");
			System.out.println();
		}
	}

	// 过滤非给定学年学期，任选课以及辅修课程
	public boolean isValidData() {
		String xn = pps.getProperty("lastTerm_XN");
		String xq = pps.getProperty("lastTerm_XQ");
		//课程性质
		if (this.getKCXZ().contains("任") && this.getKCXZ().contains("选")) {
			return false;
		}
		if (this.getKCXZ().contains("辅") && this.getKCXZ().contains("修")) {
			return false;
		}
		//学年学期
		if (xn.equals(this.getXN().trim()) && xq.equals(this.getXQ().toString())) {
			return true;
		} else {
			return false;
		}
	}

	public void parse(Text record) {
		String[] data = record.toString().split("##########");
		if (data.length > 1) {
			super.setType(data[0].trim());
			if ("1".equals(data[0].trim())) {
				return;
			} else {
				parse(data[1]);
			}
		} else {
			parse(record.toString());
		}
	}

	public String cxbkParse(String cbcj) {
		if (cbcj.contains("不")) {
			return pps.getProperty("bujige");
		} else if (cbcj.contains("格")) { // 及格或者合格
			return pps.getProperty("jige");
		}
		if (cbcj.contains("优")) {
			return pps.getProperty("you");
		}
		if (cbcj.contains("良")) {
			return pps.getProperty("liang");
		}
		if (cbcj.contains("中")) {
			return pps.getProperty("zhong");
		}
		if (cbcj.contains("缺") && cbcj.contains("考")) {
			return pps.getProperty("quekao");
		}
		if (cbcj.contains("免")) {
			return pps.getProperty("mian");
		}
		if (cbcj.contains("取") && cbcj.contains("消")) {
			return pps.getProperty("quxiao");
		}
		if (cbcj.equals("null")) {
			return "0";
		}
		return cbcj.trim();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.getXN() + "#,#" + this.getXQ() + "#,#" + this.getXKKH() + "#,#" + this.getXH() + "#,#"
				+ this.getXM() + "#,#" + this.getKCMC() + "#,#" + this.getXF() + "#,#" + this.getZSCJ() + "#,#"
				+ this.getCXBJ() + "#,#" + this.getBKCJ() + "#,#" + this.getCXCJ() + "#,#" + this.getKCXZ() + "#,#"
				+ this.getFXBJ() + "#,#" + this.getDQSZJ() + "#,#" + this.getZYFX();
	}

	public String getXN() {
		return XN;
	}

	public void setXN(String xN) {
		XN = xN;
	}

	public java.math.BigDecimal getXQ() {
		return XQ;
	}

	public void setXQ(java.math.BigDecimal xQ) {
		XQ = xQ;
	}

	public String getXKKH() {
		return XKKH;
	}

	public void setXKKH(String xKKH) {
		XKKH = xKKH;
	}

	public String getXH() {
		return XH;
	}

	public void setXH(String xH) {
		XH = xH;
	}

	public String getXM() {
		return XM;
	}

	public void setXM(String xM) {
		XM = xM;
	}

	public String getKCMC() {
		return KCMC;
	}

	public void setKCMC(String kCMC) {
		KCMC = kCMC;
	}

	public String getXF() {
		return XF;
	}

	public void setXF(String xF) {
		XF = xF;
	}

	public java.math.BigDecimal getZSCJ() {
		return ZSCJ;
	}

	public void setZSCJ(java.math.BigDecimal zSCJ) {
		ZSCJ = zSCJ;
	}

	public java.math.BigDecimal getCXBJ() {
		return CXBJ;
	}

	public void setCXBJ(java.math.BigDecimal cXBJ) {
		CXBJ = cXBJ;
	}

	public String getBKCJ() {
		return BKCJ;
	}

	public void setBKCJ(String bKCJ) {
		BKCJ = bKCJ;
	}

	public String getCXCJ() {
		return CXCJ;
	}

	public void setCXCJ(String cXCJ) {
		CXCJ = cXCJ;
	}

	public String getKCXZ() {
		return KCXZ;
	}

	public void setKCXZ(String kCXZ) {
		KCXZ = kCXZ;
	}

	public String getFXBJ() {
		return FXBJ;
	}

	public void setFXBJ(String fXBJ) {
		FXBJ = fXBJ;
	}

	public String getDQSZJ() {
		return DQSZJ;
	}

	public void setDQSZJ(String dQSZJ) {
		DQSZJ = dQSZJ;
	}

	public String getZYFX() {
		return ZYFX;
	}

	public void setZYFX(String zYFX) {
		ZYFX = zYFX;
	}

	public String getZYDM() {
		return ZYDM;
	}

	public void setZYDM(String zYDM) {
		ZYDM = zYDM;
	}

	public static Properties getPps() {
		return pps;
	}

	public static void setPps(Properties pps) {
		CJBAndXSBParser.pps = pps;
	}

	public String getKCDM() {
		return KCDM;
	}

	public void setKCDM(String kCDM) {
		KCDM = kCDM;
	}

	public String getNJ() {
		return NJ;
	}

	public void setNJ(String nJ) {
		NJ = nJ;
	}
}
