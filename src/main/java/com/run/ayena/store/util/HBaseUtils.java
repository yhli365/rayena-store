package com.run.ayena.store.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yanhong Lee
 * 
 */
public class HBaseUtils {
	private static Logger log = LoggerFactory.getLogger(HBaseUtils.class);

	public static Durability getDurability(String durability) {
		durability = durability.toUpperCase();
		if ("USE_DEFAULT".equals(durability)) {
			return Durability.USE_DEFAULT;
		} else if ("SKIP_WAL".equals(durability)) {
			return Durability.SKIP_WAL;
		} else if ("ASYNC_WAL".equals(durability)) {
			return Durability.ASYNC_WAL;
		} else if ("SYNC_WAL".equals(durability)) {
			return Durability.SYNC_WAL;
		} else if ("FSYNC_WAL".equals(durability)) {
			return Durability.FSYNC_WAL;
		} else {
			throw new IllegalArgumentException("Unknown hbase durability: "
					+ durability);
		}
	}

	public static TableName getObjectTableName(Configuration conf,
			String tableid) {
		String type = conf.get("type", "ren");
		String tablePrefix = conf.get("table.prefix");
		String name = type + tableid;
		if (StringUtils.isNotEmpty(tablePrefix)) {
			name = tablePrefix + name;
		}
		TableName tableName = TableName.valueOf(name);
		log.info("getObjectTableName#" + tableid + " => " + name);
		return tableName;
	}

}
