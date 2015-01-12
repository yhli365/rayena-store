package com.run.ayena.store.util;

import java.util.Map;

import org.apache.hadoop.mapreduce.counters.GenericCounter;

import com.google.common.collect.Maps;

/**
 * @author Yanhong Lee
 * 
 */
public class MRUtils {
	private static Map<Enum<?>, GenericCounter> counters = Maps.newHashMap();

	/**
	 * 增加计数器值.
	 * 
	 * @param name
	 * @param inc
	 */
	public static void incCounter(Enum<?> name, long incr) {
		GenericCounter c = counters.get(name);
		if (c == null) {
			c = new GenericCounter();
			counters.put(name, c);
		}
		c.increment(incr);
	}

	/**
	 * 获取所有计数器.
	 * 
	 * @param name
	 * @param inc
	 * @return
	 */
	public static Map<Enum<?>, GenericCounter> getCounters() {
		return counters;
	}

	public static void resetCounters() {
		for (GenericCounter gc : counters.values()) {
			gc.setValue(0);
		}
	}

	public static void initCounters(Enum<?>[] names) {
		for (Enum<?> name : names) {
			GenericCounter c = counters.get(name);
			if (c == null) {
				c = new GenericCounter();
				counters.put(name, c);
			}
		}
	}

}
