package com.run.ayena.store.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.ObjectPbfUtils;
import com.run.ayena.store.util.ValueUtils;

/**
 * 从kafka读取对象消息.
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectKafkaConsumer implements Runnable {
	private static final Logger log = LoggerFactory
			.getLogger(ObjectKafkaConsumer.class);

	private ConsumerConnector consumer;
	protected String topic;

	protected SimpleDateFormat df = new SimpleDateFormat(
			"yyyy/MM/dd HH:mm:ss.SSS");
	private long total = 0L;
	private int tps;
	private int tpsgap;

	protected void setup(Properties props) throws IOException {
		this.topic = props.getProperty("topic", "objectdata");
		this.tps = Integer.parseInt(props.getProperty("tps", "10"));
		this.tpsgap = Integer.parseInt(props.getProperty("tpsgap", "1000"));

		ConsumerConfig ccfg = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(ccfg);

		for (Object key : props.keySet()) {
			log.info("<conf> " + key + " = " + props.getProperty((String) key));
		}
		log.info("setup ok# topic=" + topic);
	}

	protected void shutdown() {
		try {
			if (consumer != null) {
				consumer.shutdown();
			}
			log.info("shutdown ok#");
		} catch (Exception e) {
			log.error("shutdown failed#", e);
		}

	}

	@Override
	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println("*********Results********");
		while (true) {
			log.info("Starting to receive messages: tps=" + tps + "/" + tpsgap);
			long start = System.currentTimeMillis();
			for (int i = 0; i < tps; i++) {
				if (it.hasNext()) {
					MessageAndMetadata<byte[], byte[]> mam = it.next();
					byte[] key = mam.key();
					byte[] msg = mam.message();
					total++;
					try {
						ObjectData.ObjectBase ob = ObjectData.ObjectBase.PARSER
								.parseFrom(msg);
						System.out.println("[" + df.format(new Date())
								+ "] receive: #" + (i + 1) + "/" + total + " "
								+ ValueUtils.toHexString(key) + "\n"
								+ ObjectPbfUtils.printToString(ob));
					} catch (InvalidProtocolBufferException e) {
						log.error("parse error#ObjectData.ObjectBase", e);
						return;
					} catch (UnsupportedEncodingException e) {
						log.error("parse error#ObjectData.ObjectBase", e);
						return;
					}
				}
			}
			long wait = tpsgap - (System.currentTimeMillis() - start);
			log.info("Finished to receive messages: total=" + this.total
					+ ", tps=" + tps + ", wait=" + wait);
			if (wait > 0) {
				try {
					Thread.sleep(wait);
				} catch (InterruptedException e) {
				}
			}
		}
	}

	public Properties loadConfig(String[] args) throws IOException {
		InputStream in;
		if (args.length > 0) {
			in = new FileInputStream(args[0]);
		} else {
			in = Thread
					.currentThread()
					.getContextClassLoader()
					.getResourceAsStream(
							"conf/kafka/object_consumer.properties");
		}
		Properties props = new Properties();
		props.load(in);
		return props;
	}

	public static void main(String[] args) {
		final ObjectKafkaConsumer p = new ObjectKafkaConsumer();
		try {
			Properties props = p.loadConfig(args);
			p.setup(props);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					p.shutdown();
				}
			});
			new Thread(p).start();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

}
