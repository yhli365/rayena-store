package com.run.ayena.store.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Yanhong Lee
 * 
 */
public class MetaUtils {
	public static final int VALUE_SINGLE = 1;
	public static final int VALUE_MULTI = 2;

	private static Map<String, Integer> codeTypes;

	static {
		codeTypes = new HashMap<String, Integer>();
		Configuration conf = new Configuration(false);
		conf.addResource("conf/meta.xml");
		for (String c : conf.getStringCollection("props.codes")) {
			codeTypes.put(c, VALUE_SINGLE);
		}
		for (String c : conf.getStringCollection("multiprops.codes")) {
			codeTypes.put(c, VALUE_MULTI);
		}
	}

	public static int getCodeType(String code) {
		Integer v = codeTypes.get(code);
		if (v != null) {
			return v;
		}
		return -1;
	}

	public static boolean checkCodeType(String code, int chkValue) {
		Integer v = codeTypes.get(code);
		if (v != null && v == chkValue) {
			return true;
		}
		return false;
	}

}
