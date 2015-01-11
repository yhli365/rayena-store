package com.run.ayena.store.util;

import java.util.Calendar;

/**
 * @author Yanhong Lee
 * 
 */
public final class ObjectUtils {

	public static int DEFAULT_OID_BUFFERSIZE = 4096;

	public static final byte sepByte = 1;
	public static final char sepChar = 1;

	private static Calendar cdar = Calendar.getInstance();

	/**
	 * 计算对象发现日期.
	 * 
	 * @param ts
	 * @return
	 */
	public static Integer dayValue(int ts) {
		cdar.setTimeInMillis(ts * 1000L);
		Integer dayValue = cdar.get(Calendar.YEAR) * 10000
				+ (cdar.get(Calendar.MONTH) + 1) * 100
				+ cdar.get(Calendar.DATE);
		return dayValue;
	}

}
