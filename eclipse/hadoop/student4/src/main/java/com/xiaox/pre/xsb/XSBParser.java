package com.xiaox.pre.xsb;

import java.math.BigDecimal;

import org.apache.hadoop.io.Text;

import com.xiaox.util.DataParser;

public class XSBParser extends DataParser {
	private String XH;
	private String ZYDM;
	private String ZYFX;
	private java.math.BigDecimal DQSZJ;

	public void parse(String record) {
		// 020412107#,#孙超盛#,#男#,#19940916#,#共青团员#,#汉族#,#江苏宜兴#,#江苏#,#管理学院#,#null#,#
		// 财务管理#,#0204121#,#4#,#null#,#无#,#2012#,#非师范#,#企业会计方向#,#null#,#20120915#,#
		// 江苏省宜兴中学#,#null#,#809746920@qq.com#,#051087565376,13921329376#,#null#,#32028219940916781X#,#null#,#null#,#null#,#非师范#,#
		// LDVY}Xi6CC#,#19940916#,#null#,#null#,#null#,#331#,#null#,#本二#,#00000000000000000000#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#0204#,#null#,#null#,#否#,#本科第二批#,#
		// 214266#,#null#,#0#,#否#,#null#,#null#,#null#,#是#,#null#,#null#,#
		// 20160630#,#null#,#null#,#null#,#null#,#null#,#12320282680899#,#null#,#null#,#null#,#
		// null#,#null#,#否#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#1#,#null#,#是#,#江苏省宜兴市新庄镇兴盛旅馆#,#江苏#,#1#,#null#,#15051768305#,#null#,#
		// null#,#null#,#是#,#null#,#是#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#管理学院#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#否#,#null#,#未购买#,#null#,#
		// null#,#null#,#null#,#null#,#2014-05-09
		// 07:32:23#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#
		// null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#null#,#
		// null
		try {
			String[] data = record.split("#,#");
			this.setXH(data[0].trim());
			this.setDQSZJ(new BigDecimal(data[15].trim()));
			if ("null".equals(data[17].trim())) {
				this.setZYFX("无方向");
			} else {
				this.setZYFX(data[17].trim());
			}

			this.setZYDM(data[45].trim());
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("#####################XSBParser##########################");
			System.out.println();
			e.printStackTrace();
			System.out.println();
			System.out.println(record);
			System.out.println("#######################XSBParser########################");
		}

	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.XH + "#,#" + this.DQSZJ + "#,#" + this.ZYFX + "#,#" + this.ZYDM + "#,#";
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

	public String getXH() {
		return XH;
	}

	public void setXH(String xH) {
		XH = xH;
	}

	public String getZYDM() {
		return ZYDM;
	}

	public void setZYDM(String zYDM) {
		ZYDM = zYDM;
	}

	public String getZYFX() {
		return ZYFX;
	}

	public void setZYFX(String zYFX) {
		ZYFX = zYFX;
	}

	public java.math.BigDecimal getDQSZJ() {
		return DQSZJ;
	}

	public void setDQSZJ(java.math.BigDecimal dQSZJ) {
		DQSZJ = dQSZJ;
	}

}
