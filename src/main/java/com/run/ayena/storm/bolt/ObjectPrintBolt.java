package com.run.ayena.storm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.google.protobuf.InvalidProtocolBufferException;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.ObjectPbfUtils;

public class ObjectPrintBolt implements IBasicBolt {
	private static final long serialVersionUID = 6233457114510143883L;
	private static Logger log = LoggerFactory.getLogger(ObjectPrintBolt.class);

	private long seq = 1;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context) {
		log.info("prepare ok#");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			byte[] msg = input.getBinaryByField("bytes");
			ObjectData.ObjectBase ob = ObjectData.ObjectBase.PARSER
					.parseFrom(msg);

			log.info("#{} Message: {}", seq++, ObjectPbfUtils.printToString(ob));
		} catch (InvalidProtocolBufferException e) {
			log.error("pbf parse error#", e);
		}
	}

	@Override
	public void cleanup() {
		log.info("cleanup ok#");
	}

}
