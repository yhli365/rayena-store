package com.run.ayena.store.util;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
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

	public static SequenceFile.CompressionType getCompressionType(
			Configuration conf) {
		String str = conf.get("compression.type", "block");
		CompressionType compress = CompressionType.valueOf(str.toUpperCase());
		return compress;
	}

	public static CompressionCodec getCompressionCodec(Configuration conf) {
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
		CompressionCodec codec = codecFactory.getCodecByName(conf.get("codec",
				"SnappyCodec")); // LzoCodec,SnappyCodec
		if (codec == null) {
			codec = new DefaultCodec();
		}
		return codec;
	}

	public static Configuration newConfiguration(Properties props) {
		Configuration conf = new Configuration();
		for (Object key : props.keySet()) {
			conf.set(String.valueOf(key), String.valueOf(props.get(key)));
		}
		return conf;
	}

}
