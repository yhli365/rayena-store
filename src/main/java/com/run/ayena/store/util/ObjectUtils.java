package com.run.ayena.store.util;

import java.util.Calendar;

/**
 * @author Yanhong Lee
 * 
 */
public final class ObjectUtils {

	public static final byte sepByte = 1;
	public static final char sepChar = 1;

	/**
	 * 对象档案状态: 是否已存储
	 */
	public static final byte OAS_STORE = 0b00000001;

	/**
	 * 对象档案状态: 是否已归并
	 */
	public static final byte OAS_MERGED = 0b00000010;

	/**
	 * 对象档案状态: 新发现对象
	 */
	public static final byte OAS_NEW = 0b00000100;

	/**
	 * 对象档案状态: 已存储且未改变
	 */
	public static final byte OAS_UNCHANGED = OAS_STORE | OAS_MERGED;

	private static Calendar cdar = Calendar.getInstance();

	/**
	 * 判断对象档案状态
	 * 
	 * @param captureTime
	 * @return
	 */
	public static int oas(int ts) {
		if (ts > 100) {// 临时对象
			return OAS_NEW;
		}
		return ts;
	}

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
