package com.run.ayena.store.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Maps;

/**
 * @author Yanhong Lee
 * 
 */
public class MRUtils {
	private static Map<Enum<?>, GenericCounter> counters = Maps.newHashMap();

	public static void initJobConf(Configuration conf, Tool tool) {
		String key = MRJobConfig.JOB_NAME;
		String val;
		if (conf.get(key) == null) {
			val = tool.getClass().getSimpleName() + "_"
					+ new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
			conf.set(key, val);
		}
	}

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

}
