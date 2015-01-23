package com.run.ayena.storm;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
		for (Object key : prop.keySet()) {
			conf.put((String) key, prop.get(key));
		}

		if (prop.containsKey(Config.TOPOLOGY_ACKER_EXECUTORS)) {
			Config.setNumAckers(
					conf,
					JStormUtils.parseInt(
							conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1));
		}

		if (prop.containsKey(Config.TOPOLOGY_MAX_SPOUT_PENDING)) {
			Config.setNumAckers(
					conf,
					JStormUtils.parseInt(
							conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 1));
		}

		// conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
		// conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
		// conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS),
				1);
		conf.put(Config.TOPOLOGY_WORKERS, workerNum);

		return conf;
	}

	public static void submitTopology() {

	}

	public static int getInt(Config conf, String key, int defaultValue) {
		return JStormUtils.parseInt(conf.get(key), defaultValue);
	}

	public static String getString(Config conf, String key, String defaultValue) {
		Object val = conf.get(key);
		if (val == null) {
			return defaultValue;
		}
		if (val instanceof String) {
			return (String) val;
		}

		return String.valueOf(val);
	}

	public static boolean local_mode(Config conf) {
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

}
