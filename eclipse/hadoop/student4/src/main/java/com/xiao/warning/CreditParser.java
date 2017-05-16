package com.xiao.warning;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;

import org.apache.hadoop.io.Text;

import com.xiaox.util.DataParser;

public class CreditParser extends DataParser {
	private String XN;
	private java.math.BigDecimal XQ;
	private String XKKH;
	private String XH;
	private String NJ;
	private String BJ;
	private String ZY;
	private String XM;
	private String KCMC;
	private String XF;
	private java.math.BigDecimal ZSCJ;
	private java.math.BigDecimal CXBJ;
	private String BKCJ;
	private String CXCJ;
	private String KCXZ;
	private String FXBJ;
	private String KCBH;
	private String JD;
	private String DQSZJ;
	private String SFXWK;
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
		// parser.getKCMC().trim() + "#,#" + xn + "#,#" + xq + "#,#" +
		// parser.getXM().trim() + "#,#" +score + "#,#"
		// parser.getXF() + "#,#" + bkCounter + "#,#" + cxCounter + "#,#" + jd+
		// "#,#" + parser.getDQSZJ() + "#,#"
		// parser.getSFXWK()
		// 010112101,A0010027, 数据库应用技术（文管类）,2012-2013,2,金轶超,86.0,3.0,0,0
		// 010112101#,#A0010003#,#
		// 中国近现代史纲要#,#2012-2013#,#2#,#金轶超#,#97.0#,#2.0#,#0#,#0#,#4.7#,#2012#,#否
		// ZB6216144#,#A1650004#,#
		// 电子技术课程设计#,#2016-2017#,#1#,#周嘉琦#,#81.0#,#2#,#0#,#0#,#3.1#,#2016#,#否
		String[] data = record.split("#,#");
		try {
			this.setXH(data[0].trim());
			this.setZY(data[0].trim().substring(0, data[0].trim().length() - 5));
			this.setNJ(data[0].trim().substring(data[0].trim().length() - 5, data[0].trim().length() - 3));
			this.setBJ(data[0].trim().substring(data[0].trim().length() - 3, data[0].trim().length() - 2));
			this.setKCBH(data[1].trim());
			this.setKCMC(data[2].trim());
			this.setXN(data[3].trim());
			this.setXQ(BigDecimal.valueOf(Integer.parseInt(data[4].trim())));
			this.setXM(data[5].trim());
			this.setZSCJ(BigDecimal.valueOf(Double.parseDouble(data[6])));
			this.setXF(data[7].trim());
			this.setJD(data[10].trim());
			this.setDQSZJ(data[11].trim());
			this.setSFXWK(data[12].trim());
			this.setKCXZ(data[13].trim());
		} catch (Exception e) {
			System.out.println("###############CreditParser################");
			e.printStackTrace();
			System.out.println();
			System.out.println(record);
			System.out.println("###############CreditParser################");
		}
	}

	//过滤学年，学期，课程性质不符的数据
	public boolean isValidData() {
		String xn = pps.getProperty("lastTerm_XN");
		String xq = pps.getProperty("lastTerm_XQ");
		int year1 = Integer.parseInt(xn.split("-")[0]);
		int year2 = Integer.parseInt(this.getXN().split("-")[0]);
		//课程性质
		if(this.getKCXZ().contains("任")&&this.getKCXZ().contains("选")){
			return false;
		}
		if(this.getKCXZ().contains("辅")&&this.getKCXZ().contains("修")){
			return false;
		}
		//学年学期
		if ("1".equals(xq)) {
			if (year1 == year2 && "1".equals(this.getXQ())) {
				return true;
			} else if (year1 > year2) {
				return true;
			} else {
				return false;
			}
		} else if ("2".equals(xq)) {
			if (year1 >= year2) {
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

	public String getKCBH() {
		return KCBH;
	}

	public void setKCBH(String kCBH) {
		KCBH = kCBH;
	}

	public String getBJ() {
		return BJ;
	}

	public void setBJ(String bJ) {
		BJ = bJ;
	}

	public String getNJ() {
		return NJ;
	}

	public void setNJ(String nJ) {
		NJ = nJ;
	}

	public String getZY() {
		return ZY;
	}

	public void setZY(String zY) {
		ZY = zY;
	}

	public String getJD() {
		return JD;
	}

	public void setJD(String jD) {
		JD = jD;
	}

	public String getSFXWK() {
		return SFXWK;
	}

	public void setSFXWK(String sFXWK) {
		SFXWK = sFXWK;
	}

	public String getDQSZJ() {
		return DQSZJ;
	}

	public void setDQSZJ(String dQSZJ) {
		DQSZJ = dQSZJ;
	}
}
