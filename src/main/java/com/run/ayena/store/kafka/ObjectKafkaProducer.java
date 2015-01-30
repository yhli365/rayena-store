package com.run.ayena.store.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.MRUtils;
import com.run.ayena.store.util.ObjectDataGenerator;
import com.run.ayena.store.util.ObjectPbfUtils;

/**
 * 往kafka放置对象消息.
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectKafkaProducer implements Runnable {
	private static final Logger log = LoggerFactory
			.getLogger(ObjectKafkaProducer.class);

	private kafka.javaapi.producer.Producer<byte[], byte[]> producer;
	protected String topic;

	private long total = 0L;
	private int tps;
	private int tpsgap;
	private Configuration conf;
	private ObjectDataGenerator odg;

	protected void setup(Properties props) throws IOException {
		this.topic = props.getProperty("topic", "objectdata");
		this.tps = Integer.parseInt(props.getProperty("tps", "10"));
		this.tpsgap = Integer.parseInt(props.getProperty("tpsgap", "1000"));
		conf = MRUtils.newConfiguration(props);
		odg = new ObjectDataGenerator();
		odg.setup(conf);

		ProducerConfig pcfg = new ProducerConfig(props);
		this.producer = new kafka.javaapi.producer.Producer<byte[], byte[]>(
				pcfg);

		for (Object key : props.keySet()) {
			log.info("<conf> " + key + " = " + props.getProperty((String) key));
		}
		log.info("setup ok# topic=" + topic);
	}

	protected void shutdown() {
		try {
			if (producer != null) {
				producer.close();
			}
			odg.cleanup(conf);
			log.info("shutdown ok#");
		} catch (Exception e) {
			log.error("shutdown failed#", e);
		}

	}

	public void send() throws IOException {
		ObjectData.ObjectBase ob = odg.genBase();
		byte[] data = ob.toByteArray();
		byte[] md5 = ObjectPbfUtils.md5(ob);
		producer.send(new KeyedMessage<byte[], byte[]>(topic, md5, data));
	}

	@Override
	public void run() {
		try {
			while (true) {
				long start = System.currentTimeMillis();
				for (int i = 0; i < tps; i++) {
					send();
				}
				total += tps;
				long wait = tpsgap - (System.currentTimeMillis() - start);
				System.out.println("Sending: total=" + this.total + ", tps="
						+ tps + ", wait=" + wait);
				if (wait > 0) {
					try {
						Thread.sleep(wait);
					} catch (InterruptedException e) {
					}
				}
			}
		} catch (IOException e) {
			log.error("send failed#", e);
			return;
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
							"conf/kafka/object_producer.properties");
		}
		Properties props = new Properties();
		props.load(in);
		return props;
	}

	public static void main(String[] args) {
		final ObjectKafkaProducer p = new ObjectKafkaProducer();
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
