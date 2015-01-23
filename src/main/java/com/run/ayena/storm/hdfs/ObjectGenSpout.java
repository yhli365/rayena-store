package com.run.ayena.storm.hdfs;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.ObjectDataGenerator;
import com.run.ayena.store.util.ObjectPbfUtils;

public class ObjectGenSpout extends BaseRichSpout {
	private static Logger log = LoggerFactory.getLogger(ObjectGenSpout.class);

	private static final long serialVersionUID = -4418478059937910059L;

	private ConcurrentHashMap<UUID, Values> pending;
	private SpoutOutputCollector collector;

	private int count = 0;
	private long total = 0L;

	private Configuration conf;
	private ObjectDataGenerator odg;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("md5", "data"));
	}

	public void open(@SuppressWarnings("rawtypes") Map config,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.pending = new ConcurrentHashMap<UUID, Values>();

		conf = new Configuration(false);
		odg = new ObjectDataGenerator();
		try {
			odg.setup(conf);
		} catch (IOException e) {
			throw new FailedException("odg.setup", e);
		}
		log.info("open ok.");
	}

	@Override
	public void close() {
		try {
			odg.cleanup(conf);
		} catch (IOException e) {
			throw new FailedException("odg.setup", e);
		}
		log.info("close ok.");
	}

	public void nextTuple() {
		ObjectData.ObjectBase ob;
		try {
			ob = odg.genBase();
		} catch (IOException e) {
			throw new FailedException("odg.genBase", e);
		}
		byte[] data = ob.toByteArray();
		byte[] md5 = ObjectPbfUtils.md5(ob);

		Values values = new Values(md5, data);
		UUID msgId = UUID.randomUUID();
		this.pending.put(msgId, values);
		this.collector.emit(values, msgId);

		count++;
		total++;
		if (count > 15) {
			count = 0;
			System.out.println("Pending count: " + this.pending.size()
					+ ", total: " + this.total);
			waitForSeconds(5);
		}

	}

	public void ack(Object msgId) {
		// log.info("ACK");
		this.pending.remove(msgId);
	}

	public void fail(Object msgId) {
		log.info("fail**** RESENDING FAILED TUPLE: " + msgId);
		this.collector.emit(this.pending.get(msgId), msgId);
	}

	public void waitForSeconds(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
		}
	}

}
