package com.xiaox.pre.kcb;

import org.apache.hadoop.io.Text;

import com.xiaox.util.DataParser;

public class KCParser extends DataParser{
	private String ZYDM;
	private String ZYMC;
	private String KCDM;
	private String KCMC;
	private String KCXZ;
	private String SFXWK;
	private String NJ;
	private String ZYFX;

	public void parse(String record) {
		// 2009Y122#,#Y122#,#软件工程（嵌入式软件人才培养）#,#2009#,#Y0010003#,#中国近现代史纲要#,#2.0#,#2.0-0.0#,#必修#,#普通教育#,#
		// null#,#考查#,#1#,#16#,#人文学院#,#null#,#无方向#,#null#,#null#,#3#,#
		// null#,#0#,#null#,#null#,#36#,#36#,#0#,#0#,#null#,#01-16#,#
		// null#,#null#,#理论#,#null#,#null#,#null#,#0#,#0#,#否#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#0#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#0#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null
		try{
			String[] data = record.split("#,#");
			this.setZYDM(data[1].toString());
			this.setZYMC(data[2].toString());
			this.setNJ(data[3].toString());
			this.setKCDM(data[4].trim());
			this.setKCMC(data[5].trim());
			this.setKCXZ(data[8].trim());
			this.setZYFX("null".equals(data[16].trim())?"无方向":data[16].trim());
			this.setSFXWK(data[38].trim());
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println("###############################################");
			e.printStackTrace();
			System.out.println();
			System.out.println(record);
			System.out.println("###############################################");
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
			if ("1".equals(data[0].trim())) {
				return;
			} else {
				parse(data[1]);
			}
		} else {
			parse(record.toString());
		}
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.getZYDM()+"#,#"+this.getZYMC()+"#,#"+this.getKCDM()+"#,#"+this.getKCMC()+"#,#"+this.getKCXZ()+"#,#"+this.getSFXWK();
	}
	public String getZYDM() {
		return ZYDM;
	}

	public void setZYDM(String zYDM) {
		ZYDM = zYDM;
	}

	public String getZYMC() {
		return ZYMC;
	}

	public void setZYMC(String zYMC) {
		ZYMC = zYMC;
	}

	public String getKCDM() {
		return KCDM;
	}

	public void setKCDM(String kCDM) {
		KCDM = kCDM;
	}

	public String getKCMC() {
		return KCMC;
	}

	public void setKCMC(String kCMC) {
		KCMC = kCMC;
	}

	public String getKCXZ() {
		return KCXZ;
	}

	public void setKCXZ(String kCXZ) {
		KCXZ = kCXZ;
	}

	public String getSFXWK() {
		return SFXWK;
	}

	public void setSFXWK(String sFXWK) {
		SFXWK = sFXWK;
	}

	public String getNJ() {
		return NJ;
	}

	public void setNJ(String nJ) {
		NJ = nJ;
	}

	public String getZYFX() {
		return ZYFX;
	}

	public void setZYFX(String zYFX) {
		ZYFX = zYFX;
	}

}
