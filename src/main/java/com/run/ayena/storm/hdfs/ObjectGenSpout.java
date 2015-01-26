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
import com.run.ayena.storm.StormUtils;

public class ObjectGenSpout extends BaseRichSpout {
	private static Logger log = LoggerFactory.getLogger(ObjectGenSpout.class);

	private static final long serialVersionUID = -4418478059937910059L;

	private ConcurrentHashMap<UUID, Values> pending;
	private SpoutOutputCollector collector;

	private long total = 0L;
	private int tps;
	private int tpsgap;

	private Configuration hconf;
	private ObjectDataGenerator odg;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("md5", "data"));
	}

	@SuppressWarnings("unchecked")
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.pending = new ConcurrentHashMap<UUID, Values>();

		this.tps = StormUtils.getInt(conf, "spout.tps", 100);
		this.tpsgap = StormUtils.getInt(conf, "spout.tps.gap", 1000);
		log.info("<conf> spout.tps = {}", tps);
		log.info("<conf> spout.tps.gap = {}", tpsgap);
		Configuration hconf = StormUtils.getHdfsConfiguration(conf);
		odg = new ObjectDataGenerator();
		try {
			odg.setup(hconf);
		} catch (IOException e) {
			throw new FailedException("odg.setup", e);
		}
		log.info("open ok.");
	}

	@Override
	public void close() {
		try {
			odg.cleanup(hconf);
		} catch (IOException e) {
			throw new FailedException("odg.setup", e);
		}
		log.info("close ok.");
	}

	public void nextTuple() {
		long start = System.currentTimeMillis();
		try {
			for (int i = 0; i < tps; i++) {
				ObjectData.ObjectBase ob = odg.genBase();

				byte[] data = ob.toByteArray();
				byte[] md5 = ObjectPbfUtils.md5(ob);

				Values values = new Values(md5, data);
				UUID msgId = UUID.randomUUID();
				this.pending.put(msgId, values);
				this.collector.emit(values, msgId);

				total++;
			}
		} catch (IOException e) {
			throw new FailedException("odg.genBase", e);
		}

		long wait = tpsgap - (System.currentTimeMillis() - start);
		System.out.println("Pending count: " + this.pending.size()
				+ ", total: " + this.total + ", wait: " + wait);
		if (wait > 0) {
			try {
				Thread.sleep(wait);
			} catch (InterruptedException e) {
			}
			System.out.println("Pending count_awake: " + this.pending.size()
					+ ", total: " + this.total);
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

}
