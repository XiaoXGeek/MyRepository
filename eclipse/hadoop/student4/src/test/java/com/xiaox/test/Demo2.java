package com.xiaox.test;

import java.math.BigDecimal;

public class Demo2 {
	public static void main(String[] args){
		BigDecimal a = new BigDecimal(2.0);
		if("2".equals(a.toString())){
			System.out.println("YES");
		}else{
			System.out.println(a.toString());
		}
	}
}
