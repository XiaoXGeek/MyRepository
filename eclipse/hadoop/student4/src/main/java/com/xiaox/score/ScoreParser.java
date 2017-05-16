package com.xiaox.score;

import java.math.BigDecimal;

import org.apache.hadoop.io.Text;

public class ScoreParser {
	private String XN;
	private java.math.BigDecimal XQ;
	private String XKKH;
	private String XH;
	private String NJ;
	private String BJ;
	private String ZYDM;
	private String XM;
	private String KCMC;
	private String XF;
	private java.math.BigDecimal ZSCJ;
	private java.math.BigDecimal CXBJ;
	private String BKCJ;
	private String CXCJ;
	private String KCXZ;
	private String FXBJ;
	private String KCDM;
	private String DQSZJ;
	private String ZYFX;
	private String SFXWK;

	public void parse(String record) {
		// cjbParser.getXN() + "#,#" + cjbParser.getXQ() + "#,#" +
		// cjbParser.getXKKH()
		// "#,#" + cjbParser.getXH() + "#,#" + cjbParser.getXM() + "#,#" +
		// cjbParser.getKCMC()
		// "#,#" + cjbParser.getXF() + "#,#" + cjbParser.getZSCJ() + "#,#" +
		// cjbParser.getCXBJ()
		// "#,#" + cjbParser.getBKCJ() + "#,#" + cjbParser.getCXCJ() + "#,#" +
		// cjbParser.getKCXZ()
		// "#,#" + cjbParser.getFXBJ() + "#,#" + cjbParser.getDQSZJ() + "#,#" +
		// cjbParser.getZYFX()
		// "#,#" + ("null".equals(kcParser.getSFXWK()) ? "否" :
		// kcParser.getSFXWK()));
		// 2012-2013#,#2#,#(2012-2013-2)-A0010003-201100001-1#,#010112122#,#金澜#,#
		// 中国近现代史纲要#,#2.0#,#94.0#,#0.0#,#0#,#
		// 0#,#null#,#0#,#2012#,#无方向#,#
		// 否
		// 2014-2015#,#1#,#(2014-2015-1)-P0010019-200800028-5#,#010212101#,#黄阳#,#体育（5）-早锻炼俱乐部#,#0.5#,#62.0#,#0.0#,#0#,#0#,#null#,#null#,#2012#,#无方向#,#否
		String[] data = record.split("#,#");
		try {
			this.setXN(data[0].trim());
			this.setXQ(BigDecimal.valueOf(Integer.parseInt(data[1])));
			this.setXKKH(data[2].trim());
			// course id
			// 2012-2013,2,(2012-2013-2)-A0010060-201200030-1,072311115,刘夏,德语（4）,8.0,71.0,0,0,0,null,0

			this.setKCDM(data[2].trim().substring(14, 22));

			this.setXH(data[3].trim());
			this.setZYDM(data[3].trim().substring(0, data[3].trim().length() - 5));
			this.setNJ(data[3].trim().substring(data[3].trim().length() - 5, data[3].trim().length() - 3));
			this.setBJ(data[3].trim().substring(data[3].trim().length() - 3, data[3].trim().length() - 2));
			this.setXM(data[4].trim());
			this.setKCMC(data[5].trim());
			this.setXF(data[6].trim());
			if ("null".equals(data[7].trim())) {
				this.setZSCJ(new BigDecimal(0));
			} else {
				this.setZSCJ(BigDecimal.valueOf(Double.parseDouble(data[7])));
			}
			if ("0".equals(data[8].trim()) || "0.0".equals(data[8].trim())) {
				this.setCXBJ(new BigDecimal(0));
			} else if ("1".equals(data[8].trim())) {
				this.setCXBJ(new BigDecimal(1));
			}

			this.setBKCJ(data[9].trim());
			if (data[10] == null) {
				this.setCXCJ("0");
			} else if ("null".equals(data[10].trim())) {
				this.setCXCJ("0");
			} else {
				this.setCXCJ(data[10].trim());
			}
			this.setKCXZ(data[11].trim());
			this.setFXBJ(data[12].trim());
			this.setDQSZJ(data[13].trim());
			this.setZYFX(data[14].trim());
			this.setSFXWK(data[15].trim());
		} catch (Exception e) {
			System.out.println(record);
		}
	}

	public boolean isValidData() {
		return true;
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.getKCMC() + "#,#" + this.getXN() + "#,#" + this.getXQ() + "#,#" + this.getXM().trim() + "#,#"
				+ this.getDQSZJ() + "#,#" + this.getCXBJ() + "#,#" + this.getSFXWK() + "#,#" + this.getKCXZ();
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

	public String getNJ() {
		return NJ;
	}

	public void setNJ(String nJ) {
		NJ = nJ;
	}

	public String getBJ() {
		return BJ;
	}

	public void setBJ(String bJ) {
		BJ = bJ;
	}

	public String getKCDM() {
		return KCDM;
	}

	public void setKCDM(String kCDM) {
		KCDM = kCDM;
	}

	public String getZYDM() {
		return ZYDM;
	}

	public void setZYDM(String zYDM) {
		ZYDM = zYDM;
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

	public String getSFXWK() {
		return SFXWK;
	}

	public void setSFXWK(String sFXWK) {
		SFXWK = sFXWK;
	}

}
