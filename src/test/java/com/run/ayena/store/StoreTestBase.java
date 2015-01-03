package com.run.ayena.store;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yanhong Lee
 * 
 */
public class StoreTestBase {
	private static final Logger log = LoggerFactory
			.getLogger(StoreTestBase.class);

	public void driver(String cmd) throws IOException {
		log.info("driver cmd: " + cmd);
		System.setProperty("hadoop.home.dir", "D:/ycloud/hadoop-2.5.0-cdh5.2.0");
		String[] args;
		if (StringUtils.isEmpty(cmd)) {
			args = new String[0];
		} else {
			args = cmd.split("\\s+");
		}

		try {
			StoreAppDriver.exec(args);
		} catch (Exception e) {
			throw new IOException("exec failed: " + cmd, e);
		}
	}

}
