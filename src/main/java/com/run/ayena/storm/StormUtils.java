package com.run.ayena.storm;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.utils.JStormUtils;

public class StormUtils {
	private static Logger log = LoggerFactory.getLogger(StormUtils.class);

	public static Config loadConfig(String file) throws IOException {
		Properties prop = new Properties();
		InputStream stream = new FileInputStream(file);
		prop.load(stream);

		Config conf = new Config();
		List<String> keys = new ArrayList<String>();
		for (Object key : prop.keySet()) {
			conf.put((String) key, prop.get(key));
			keys.add((String) key);
		}
		Collections.sort(keys);
		for (String key : keys) {
			log.info("<conf> {} = {}", key, prop.get(key));
		}

		if (prop.containsKey(Config.TOPOLOGY_ACKER_EXECUTORS)) {
			Config.setNumAckers(
					conf,
					JStormUtils.parseInt(
							conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1));
		}

		if (prop.containsKey(Config.TOPOLOGY_MAX_SPOUT_PENDING)) {
			Config.setMaxSpoutPending(
					conf,
					JStormUtils.parseInt(
							conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 1));
		}

		if (prop.containsKey(Config.TOPOLOGY_MAX_TASK_PARALLELISM)) {
			int vi = JStormUtils.parseInt(
					conf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM), 6);
			conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, vi);
		}

		if (prop.containsKey(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
			int vi = JStormUtils.parseInt(
					conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 20);
			conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, vi);
		}

		int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS),
				1);
		conf.put(Config.TOPOLOGY_WORKERS, workerNum);

		return conf;
	}

	public static int getInt(Map<String, Object> conf, String key,
			int defaultValue) {
		return JStormUtils.parseInt(conf.get(key), defaultValue);
	}

	public static String getString(Map<String, Object> conf, String key,
			String defaultValue) {
		Object val = conf.get(key);
		if (val == null) {
			return defaultValue;
		}
		if (val instanceof String) {
			return (String) val;
		}

		return String.valueOf(val);
	}

	public static boolean local_mode(Map<String, Object> conf) {
		String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
		if (mode != null) {
			if (mode.equals("local")) {
				return true;
			}
		}

		return false;
	}

	public static void runTopology(Config conf, TopologyBuilder builder)
			throws AlreadyAliveException, InvalidTopologyException {
		String name = getString(conf, Config.TOPOLOGY_NAME, "test-topo");
		if (local_mode(conf)) {
			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology(name, conf, builder.createTopology());
			int secs = getInt(conf, "storm.cluster.local.sleeps", 300);
			waitForSeconds(secs);
			cluster.killTopology(name);
			cluster.shutdown();
			System.exit(0);
		} else {
			StormSubmitter.submitTopology(name, conf, builder.createTopology());
		}
	}

	public static void waitForSeconds(int seconds) {
		try {
			log.info("waitForSeconds# begin=" + seconds);
			Thread.sleep(seconds * 1000);
			log.info("waitForSeconds# end=" + seconds);
		} catch (InterruptedException e) {
		}
	}

	public static Configuration getHdfsConfiguration(
			@SuppressWarnings("rawtypes") Map conf) {
		Configuration hconf = new Configuration();
		for (Object key : conf.keySet()) {
			hconf.set(String.valueOf(key), String.valueOf(conf.get(key)));
		}
		log.info("<hconf> fs.defaultFS = {}", hconf.get("fs.defaultFS"));
		return hconf;
	}

}
