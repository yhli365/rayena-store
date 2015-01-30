package com.run.ayena.storm.topo;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.kafka.KafkaSpout;
import com.run.ayena.storm.StormUtils;
import com.run.ayena.storm.bolt.ObjectPrintBolt;

/**
 * 从kafka读取消息，收集后上传到hdfs.
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectPrintKafkaTopology {

	static final String SPOUT_ID = "ogkafka-spout";
	static final String BOLT_ID = "ogprint-bolt";

	public static void main(String[] args) throws Exception {

		Config conf = StormUtils.loadConfig(args,
				"conf/storm/ogprint_local.prop");

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT_ID, new KafkaSpout(),
				StormUtils.getInt(conf, "spout.parallel", 3));
		builder.setBolt(BOLT_ID, new ObjectPrintBolt(),
				StormUtils.getInt(conf, "bolt.parallel", 2)).shuffleGrouping(
				SPOUT_ID);

		StormUtils.runTopology(conf, builder);
	}

}
