package com.run.ayena.store.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;

/**
 * @author Yanhong Lee
 * 
 */
public class MRUtils {

	public static void initJobConf(Configuration conf, Tool tool) {
		String key = MRJobConfig.JOB_NAME;
		String val;
		if (conf.get(key) == null) {
			val = tool.getClass().getSimpleName() + "_"
					+ new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
			conf.set(key, val);
		}
	}

}
