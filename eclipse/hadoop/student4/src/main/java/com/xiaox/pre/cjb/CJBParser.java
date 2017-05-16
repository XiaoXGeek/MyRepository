package com.xiaox.pre.cjb;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;

import org.apache.hadoop.io.Text;

import com.xiaox.util.DataParser;

public class CJBParser extends DataParser {
	private String XN;
	private java.math.BigDecimal XQ;
	private String XKKH;
	private String XH;
	// 专业
	private String ZY;
	// 年级
	private String NJ;
	// 班级
	private String BJ;
	private String XM;
	private String KCMC;
	// 课程编号
	private String KCBH;
	private java.math.BigDecimal QZXS;
	private String XF;
	private String CJ;
	private java.math.BigDecimal ZSCJ;
	private java.math.BigDecimal JD;
	private String BZ;
	private String XGSJ;
	private String XGS;
	private java.math.BigDecimal CXBJ;
	private String TZF;
	private java.math.BigDecimal TZFJD;
	private String KCDM;
	private String PSCJ;
	private String QMCJ;
	private String SYCJ;
	private String BKCJ;
	private String CXCJ;
	private String KCXZ;
	private java.math.BigDecimal TJ;
	private java.math.BigDecimal TJBZ;
	private java.math.BigDecimal TZJD;
	private String CXXNXQ;
	private String QZCJ;
	private String FXBJ;
	private java.math.BigDecimal JF;
	private String KCGS;
	private String XSQR;
	private String BZXX;
	private String DXQJL;
	private String BKCJ_TZF;
	private String BKCJ_BZ;
	private String LLCJ;
	private String LLZSCJ;
	private String XMDM;
	private String XMMC;
	private String QMCJ_BF;
	private String GAXS;
	private String JYCJ;
	private String JYBKCJ;
	private String FJF;
	private String CJJF_BZ;
	private String ZHXS;
	private String CJ1;
	private String CJ2;
	private String CJ3;
	private String CJ4;
	private String CJ5;
	private String CJ6;
	private String CJ7;
	private String CJ8;
	private String CJ9;
	private String CJ10;
	private String PY;
	private String SFZF;
	private String BK_SFDY;
	private String KHFS;
	private String SFKC;
	private String SFQCJ;
	private String SFQBKCJ;
	private String CJSFBJ;
	private String SFQCXCJ;
	private String CJSFSJ;
	private String YSBKCJ;
	private java.math.BigDecimal BKJF;
	private String YSCJ;
	private String SMXX;
	private String HKLSBJ;
	private String CGCJBZ;
	private String CJSFCZR;
	private String JDCL_BZ;
	private String DZCJ;
	private String KCLB_Z;
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
		try {
			// 2012-2013, 2, (2012-2013-2)-A0010060-201200030-1, 072311115, 刘夏,
			// 德语（4）, null, 8.0, 71, 71,
			// 2.1, null, null, null, 0,
			// null, null, null, 92, 62,
			// null,null,null,必修,null,null,null,null,null,0,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
			// 010112101,A0010027, 数据库应用技术（文管类）,2012-2013,2,金轶超,86.0,3.0,0,0
			String[] data = record.split("#,#");
			this.setXN(data[0].trim());
			this.setXQ(BigDecimal.valueOf(Integer.parseInt(data[1].trim())));
			if (data[2].trim().length() < 34) {
				String str = data[2].trim();
				// 简单补0
				for (int i = data[2].trim().length(); i < 34; i++) {
					str += "0";
				}
				this.setXKKH(str);
				if (data[2].trim().length() >= 22) {
					this.setKCBH(data[2].trim().substring(14, 22));
				} else {
					this.setKCBH("00000000");
				}
			} else {
				this.setXKKH(data[2].trim());
				this.setKCBH(data[2].trim().substring(14, 22));
			}

			this.setXH(data[3].trim());
			this.setZY(data[3].trim().substring(0, data[3].trim().length() - 5));
			this.setNJ("20" + data[3].trim().substring(data[3].trim().length() - 5, data[3].trim().length() - 3));
			this.setBJ(data[3].trim().substring(data[3].trim().length() - 3, data[3].trim().length() - 2));

			this.setXM(data[4].trim());
			this.setKCMC(data[5].trim());
			// data[6]不知道是啥玩意
			this.setXF(data[7].trim());
			this.setCJ(data[8].trim());

			// 对折算成绩的几种处理
			if ("null".equals(data[9].trim())) { // 折算成绩是"null",就赋值为0
				this.setZSCJ(new BigDecimal(0));
			} else {
				this.setZSCJ(BigDecimal.valueOf(Double.parseDouble(data[9])));
			}

			if ("null".equals(data[10].trim())) {
				this.setJD(new BigDecimal(0));
			} else {
				this.setJD(BigDecimal.valueOf(Double.parseDouble(data[10].trim())));
			}

			if ("0".equals(data[14].trim()) || "null".equals(data[14].trim())) {
				this.setCXBJ(new BigDecimal(0));
				this.setBKCJ(cxbkParse(data[21].trim()));
				// 重修成绩
				this.setCXCJ(cxbkParse(data[22].trim()));
				this.setKCXZ(data[23].trim());
				this.setFXBJ(data[29].trim());
			} else if ("1".equals(data[14].trim())) {
				this.setCXBJ(new BigDecimal(1));
				this.setBKCJ(cxbkParse(data[21].trim()));
				// 重修成绩
				this.setCXCJ(cxbkParse(data[22].trim()));
				this.setKCXZ(data[23].trim());
				this.setFXBJ(data[29].trim());
			}
		} catch (Exception e) {
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++");
			e.printStackTrace();
			System.out.println();
			System.out.println(record);
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++");
			System.out.println();
		}
	}

	public boolean isValidData() {
		return true;
	}

	public void parse(Text record) {
		String[] data = record.toString().split("##########");
		if (data.length > 1) {
			super.setType(data[0].trim());
			if("0".equals(data[0].trim())){
				return;
			}else{
				parse(data[1]);
			}
		} else {
			parse(record.toString());
		}
	}

	//将成绩信息中的数据转换为数值信息
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
				+ this.getFXBJ();
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

	public java.math.BigDecimal getQZXS() {
		return QZXS;
	}

	public void setQZXS(java.math.BigDecimal qZXS) {
		QZXS = qZXS;
	}

	public String getXF() {
		return XF;
	}

	public void setXF(String xF) {
		XF = xF;
	}

	public String getCJ() {
		return CJ;
	}

	public void setCJ(String cJ) {
		CJ = cJ;
	}

	public java.math.BigDecimal getZSCJ() {
		return ZSCJ;
	}

	public void setZSCJ(java.math.BigDecimal zSCJ) {
		ZSCJ = zSCJ;
	}

	public java.math.BigDecimal getJD() {
		return JD;
	}

	public void setJD(java.math.BigDecimal jD) {
		JD = jD;
	}

	public String getBZ() {
		return BZ;
	}

	public void setBZ(String bZ) {
		BZ = bZ;
	}

	public String getXGSJ() {
		return XGSJ;
	}

	public void setXGSJ(String xGSJ) {
		XGSJ = xGSJ;
	}

	public String getXGS() {
		return XGS;
	}

	public void setXGS(String xGS) {
		XGS = xGS;
	}

	public java.math.BigDecimal getCXBJ() {
		return CXBJ;
	}

	public void setCXBJ(java.math.BigDecimal cXBJ) {
		CXBJ = cXBJ;
	}

	public String getTZF() {
		return TZF;
	}

	public void setTZF(String tZF) {
		TZF = tZF;
	}

	public java.math.BigDecimal getTZFJD() {
		return TZFJD;
	}

	public void setTZFJD(java.math.BigDecimal tZFJD) {
		TZFJD = tZFJD;
	}

	public String getKCDM() {
		return KCDM;
	}

	public void setKCDM(String kCDM) {
		KCDM = kCDM;
	}

	public String getPSCJ() {
		return PSCJ;
	}

	public void setPSCJ(String pSCJ) {
		PSCJ = pSCJ;
	}

	public String getQMCJ() {
		return QMCJ;
	}

	public void setQMCJ(String qMCJ) {
		QMCJ = qMCJ;
	}

	public String getSYCJ() {
		return SYCJ;
	}

	public void setSYCJ(String sYCJ) {
		SYCJ = sYCJ;
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

	public java.math.BigDecimal getTJ() {
		return TJ;
	}

	public void setTJ(java.math.BigDecimal tJ) {
		TJ = tJ;
	}

	public java.math.BigDecimal getTJBZ() {
		return TJBZ;
	}

	public void setTJBZ(java.math.BigDecimal tJBZ) {
		TJBZ = tJBZ;
	}

	public java.math.BigDecimal getTZJD() {
		return TZJD;
	}

	public void setTZJD(java.math.BigDecimal tZJD) {
		TZJD = tZJD;
	}

	public String getCXXNXQ() {
		return CXXNXQ;
	}

	public void setCXXNXQ(String cXXNXQ) {
		CXXNXQ = cXXNXQ;
	}

	public String getQZCJ() {
		return QZCJ;
	}

	public void setQZCJ(String qZCJ) {
		QZCJ = qZCJ;
	}

	public String getFXBJ() {
		return FXBJ;
	}

	public void setFXBJ(String fXBJ) {
		FXBJ = fXBJ;
	}

	public java.math.BigDecimal getJF() {
		return JF;
	}

	public void setJF(java.math.BigDecimal jF) {
		JF = jF;
	}

	public String getKCGS() {
		return KCGS;
	}

	public void setKCGS(String kCGS) {
		KCGS = kCGS;
	}

	public String getXSQR() {
		return XSQR;
	}

	public void setXSQR(String xSQR) {
		XSQR = xSQR;
	}

	public String getBZXX() {
		return BZXX;
	}

	public void setBZXX(String bZXX) {
		BZXX = bZXX;
	}

	public String getDXQJL() {
		return DXQJL;
	}

	public void setDXQJL(String dXQJL) {
		DXQJL = dXQJL;
	}

	public String getBKCJ_TZF() {
		return BKCJ_TZF;
	}

	public void setBKCJ_TZF(String bKCJ_TZF) {
		BKCJ_TZF = bKCJ_TZF;
	}

	public String getBKCJ_BZ() {
		return BKCJ_BZ;
	}

	public void setBKCJ_BZ(String bKCJ_BZ) {
		BKCJ_BZ = bKCJ_BZ;
	}

	public String getLLCJ() {
		return LLCJ;
	}

	public void setLLCJ(String lLCJ) {
		LLCJ = lLCJ;
	}

	public String getLLZSCJ() {
		return LLZSCJ;
	}

	public void setLLZSCJ(String lLZSCJ) {
		LLZSCJ = lLZSCJ;
	}

	public String getXMDM() {
		return XMDM;
	}

	public void setXMDM(String xMDM) {
		XMDM = xMDM;
	}

	public String getXMMC() {
		return XMMC;
	}

	public void setXMMC(String xMMC) {
		XMMC = xMMC;
	}

	public String getQMCJ_BF() {
		return QMCJ_BF;
	}

	public void setQMCJ_BF(String qMCJ_BF) {
		QMCJ_BF = qMCJ_BF;
	}

	public String getGAXS() {
		return GAXS;
	}

	public void setGAXS(String gAXS) {
		GAXS = gAXS;
	}

	public String getJYCJ() {
		return JYCJ;
	}

	public void setJYCJ(String jYCJ) {
		JYCJ = jYCJ;
	}

	public String getJYBKCJ() {
		return JYBKCJ;
	}

	public void setJYBKCJ(String jYBKCJ) {
		JYBKCJ = jYBKCJ;
	}

	public String getFJF() {
		return FJF;
	}

	public void setFJF(String fJF) {
		FJF = fJF;
	}

	public String getCJJF_BZ() {
		return CJJF_BZ;
	}

	public void setCJJF_BZ(String cJJF_BZ) {
		CJJF_BZ = cJJF_BZ;
	}

	public String getZHXS() {
		return ZHXS;
	}

	public void setZHXS(String zHXS) {
		ZHXS = zHXS;
	}

	public String getCJ1() {
		return CJ1;
	}

	public void setCJ1(String cJ1) {
		CJ1 = cJ1;
	}

	public String getCJ2() {
		return CJ2;
	}

	public void setCJ2(String cJ2) {
		CJ2 = cJ2;
	}

	public String getCJ3() {
		return CJ3;
	}

	public void setCJ3(String cJ3) {
		CJ3 = cJ3;
	}

	public String getCJ4() {
		return CJ4;
	}

	public void setCJ4(String cJ4) {
		CJ4 = cJ4;
	}

	public String getCJ5() {
		return CJ5;
	}

	public void setCJ5(String cJ5) {
		CJ5 = cJ5;
	}

	public String getCJ6() {
		return CJ6;
	}

	public void setCJ6(String cJ6) {
		CJ6 = cJ6;
	}

	public String getCJ7() {
		return CJ7;
	}

	public void setCJ7(String cJ7) {
		CJ7 = cJ7;
	}

	public String getCJ8() {
		return CJ8;
	}

	public void setCJ8(String cJ8) {
		CJ8 = cJ8;
	}

	public String getCJ9() {
		return CJ9;
	}

	public void setCJ9(String cJ9) {
		CJ9 = cJ9;
	}

	public String getCJ10() {
		return CJ10;
	}

	public void setCJ10(String cJ10) {
		CJ10 = cJ10;
	}

	public String getPY() {
		return PY;
	}

	public void setPY(String pY) {
		PY = pY;
	}

	public String getSFZF() {
		return SFZF;
	}

	public void setSFZF(String sFZF) {
		SFZF = sFZF;
	}

	public String getBK_SFDY() {
		return BK_SFDY;
	}

	public void setBK_SFDY(String bK_SFDY) {
		BK_SFDY = bK_SFDY;
	}

	public String getKHFS() {
		return KHFS;
	}

	public void setKHFS(String kHFS) {
		KHFS = kHFS;
	}

	public String getSFKC() {
		return SFKC;
	}

	public void setSFKC(String sFKC) {
		SFKC = sFKC;
	}

	public String getSFQCJ() {
		return SFQCJ;
	}

	public void setSFQCJ(String sFQCJ) {
		SFQCJ = sFQCJ;
	}

	public String getSFQBKCJ() {
		return SFQBKCJ;
	}

	public void setSFQBKCJ(String sFQBKCJ) {
		SFQBKCJ = sFQBKCJ;
	}

	public String getCJSFBJ() {
		return CJSFBJ;
	}

	public void setCJSFBJ(String cJSFBJ) {
		CJSFBJ = cJSFBJ;
	}

	public String getSFQCXCJ() {
		return SFQCXCJ;
	}

	public void setSFQCXCJ(String sFQCXCJ) {
		SFQCXCJ = sFQCXCJ;
	}

	public String getCJSFSJ() {
		return CJSFSJ;
	}

	public void setCJSFSJ(String cJSFSJ) {
		CJSFSJ = cJSFSJ;
	}

	public String getYSBKCJ() {
		return YSBKCJ;
	}

	public void setYSBKCJ(String ySBKCJ) {
		YSBKCJ = ySBKCJ;
	}

	public java.math.BigDecimal getBKJF() {
		return BKJF;
	}

	public void setBKJF(java.math.BigDecimal bKJF) {
		BKJF = bKJF;
	}

	public String getYSCJ() {
		return YSCJ;
	}

	public void setYSCJ(String ySCJ) {
		YSCJ = ySCJ;
	}

	public String getSMXX() {
		return SMXX;
	}

	public void setSMXX(String sMXX) {
		SMXX = sMXX;
	}

	public String getHKLSBJ() {
		return HKLSBJ;
	}

	public void setHKLSBJ(String hKLSBJ) {
		HKLSBJ = hKLSBJ;
	}

	public String getCGCJBZ() {
		return CGCJBZ;
	}

	public void setCGCJBZ(String cGCJBZ) {
		CGCJBZ = cGCJBZ;
	}

	public String getCJSFCZR() {
		return CJSFCZR;
	}

	public void setCJSFCZR(String cJSFCZR) {
		CJSFCZR = cJSFCZR;
	}

	public String getJDCL_BZ() {
		return JDCL_BZ;
	}

	public void setJDCL_BZ(String jDCL_BZ) {
		JDCL_BZ = jDCL_BZ;
	}

	public String getDZCJ() {
		return DZCJ;
	}

	public void setDZCJ(String dZCJ) {
		DZCJ = dZCJ;
	}

	public String getKCLB_Z() {
		return KCLB_Z;
	}

	public void setKCLB_Z(String kCLB_Z) {
		KCLB_Z = kCLB_Z;
	}

	public String getZY() {
		return ZY;
	}

	public void setZY(String zY) {
		ZY = zY;
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

	public String getKCBH() {
		return KCBH;
	}

	public void setKCBH(String kCBH) {
		KCBH = kCBH;
	}

}
