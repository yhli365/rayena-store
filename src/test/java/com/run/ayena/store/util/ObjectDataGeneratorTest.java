package com.run.ayena.store.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.run.ayena.pbf.ObjectData;

/**
 * @author yhli
 * 
 */
public class ObjectDataGeneratorTest {

	@Test
	public void testGen() throws IOException {
		Configuration conf = new Configuration();
		ObjectDataGenerator odg = new ObjectDataGenerator();
		odg.setup(conf);
		for (int i = 0; i < 10; i++) {
			ObjectData.ObjectBase ob = odg.genBase();
			System.out.println("\n---------------#" + i + "\n"
					+ ObjectPbfUtils.printToString(ob));
		}
		odg.cleanup(conf);
	}

}
