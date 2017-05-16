package com.xiaox.test;

import java.util.HashMap;
import java.util.Map;

public class Demo3 {
	public static void main(String[] args){
		Map<String, String> map = new HashMap<String, String>();
		map.put("2012", "2012");
		if(map.get("2013")==null){
			System.out.println("YES");
		}
	}
}
