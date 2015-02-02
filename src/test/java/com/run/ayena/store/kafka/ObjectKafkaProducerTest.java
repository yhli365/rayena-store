package com.run.ayena.store.kafka;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

public class ObjectKafkaProducerTest {

	@Test
	public void sendMessages() throws IOException {
		ObjectKafkaProducer p = new ObjectKafkaProducer();
		Properties props = p.loadConfig(null);
		p.setup(props);
		for (int i = 0; i < 10; i++) {
			p.send();
		}
		p.shutdown();
	}

}
