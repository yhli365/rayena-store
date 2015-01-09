package com.run.ayena.store.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yanhong Lee
 * 
 * @param <K>
 * @param <V>
 */
public class IdentityMapper<K, V> extends Mapper<K, V, K, V> {

	private static final Logger log = LoggerFactory
			.getLogger(IdentityMapper.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		log.info("setup ok.");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		log.info("cleanup ok.");
	}

	@Override
	protected void map(K key, V value, Context context) throws IOException,
			InterruptedException {
		context.write(key, value);
	}

}
