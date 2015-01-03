package com.run.ayena.store.hbase;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.store.util.ValueUtils;

/**
 * @author yhli
 * 
 */
public class HBaseClient {
	protected Logger log = LoggerFactory.getLogger(HBaseClient.class);

	protected Configuration conf;
	protected HBaseAdmin admin;
	protected HConnection conn;

	public HBaseClient(Configuration userConf) {
		conf = HBaseConfiguration.create();
		conf.addResource(userConf);
	}

	public void close() throws IOException {
		if (admin != null) {
			admin.close();
			log.info("HBaseAdmin closed!");
		}
		if (conn != null) {
			conn.close();
			log.info("HConnection closed!");
		}
	}

	public HTableInterface getHTable(TableName tableName) throws IOException {
		if (conn == null) {
			conn = HConnectionManager.createConnection(conf);
		}
		HTableInterface table = conn.getTable(tableName);
		return table;
	}

	public HBaseAdmin getHBaseAdmin() throws IOException {
		if (admin == null) {
			admin = new HBaseAdmin(conf);
		}
		return admin;
	}

	/**
	 * 创建表.
	 * 
	 * @param desc
	 * @param splitKeys
	 * @throws IOException
	 */
	public void createTable(HTableDescriptor desc, byte[][] splitKeys)
			throws IOException {
		admin = getHBaseAdmin();
		if (admin.isTableAvailable(desc.getName())) {
			log.warn("Table create failed: table({}) is exist!",
					desc.getNameAsString());
			return;
		}
		admin.createTable(desc, splitKeys);
		int numRegs = splitKeys == null ? 1 : splitKeys.length + 1;
		log.info("Table create ok: {}, numRegs={}", desc.getNameAsString(),
				numRegs);
	}

	/**
	 * 删除表.
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void deleteTable(TableName tableName) throws IOException {
		admin = getHBaseAdmin();
		if (!admin.isTableAvailable(tableName)) {
			log.warn("Table delete failed: table({}) is not exist!", tableName);
			return;
		}
		if (admin.isTableEnabled(tableName)) {
			admin.disableTable(tableName);
		}
		admin.deleteTable(tableName);
		log.info("Table delete ok: {}", tableName);
	}

	/**
	 * 将表中memstore数据存储到HDFS上.
	 * 
	 * @param tableName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void flush(TableName tableName) throws IOException,
			InterruptedException {
		admin = getHBaseAdmin();
		if (!admin.isTableAvailable(tableName)) {
			log.warn("Table flush failed: table({}) is not exist!", tableName);
			return;
		}
		admin.flush(tableName.toBytes());
		log.info("Table flush ok: {}", tableName);
	}

	/**
	 * 统计表中行数，包括各region块行数.
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void count(TableName tableName) throws IOException {
		admin = getHBaseAdmin();
		List<HRegionInfo> regions = admin.getTableRegions(tableName);
		HTableInterface table = getHTable(tableName);
		log.info("---------------------count start: " + tableName);
		long count = 0;
		int regId = 0;
		SortedMap<String, Long> map = new TreeMap<String, Long>();
		for (HRegionInfo hr : regions) {
			Scan scan = new Scan(hr.getStartKey(), hr.getEndKey());
			scan.setCacheBlocks(false);
			scan.setCaching(100);
			scan.setBatch(10);
			ResultScanner rs = table.getScanner(scan);
			Result r;
			long sum = 0;
			SortedMap<String, Long> cfMap = new TreeMap<String, Long>();
			while ((r = rs.next()) != null) {
				for (Cell cell : r.rawCells()) {
					String cf = Bytes.toStringBinary(cell.getFamilyArray(),
							cell.getFamilyOffset(), cell.getFamilyLength());
					Long c = cfMap.get(cf);
					if (c == null) {
						c = 1L;
					} else {
						c++;
					}
					cfMap.put(cf, c);
				}
				sum += r.size();
			}
			count += sum;
			StringBuilder sb = new StringBuilder();
			sb.append("[").append(regId++).append("] ");
			sb.append(hr.getRegionNameAsString()).append(" => ");
			sb.append(sum);
			if (!cfMap.isEmpty()) {
				sb.append(" (");
				for (String cf : cfMap.keySet()) {
					sb.append(cf).append("=").append(cfMap.get(cf))
							.append(", ");

					Long c = map.get(cf);
					if (c == null) {
						c = cfMap.get(cf);
					} else {
						c += cfMap.get(cf);
					}
					map.put(cf, c);
				}
				sb.delete(sb.length() - 2, sb.length());
				sb.append(")");
			}
			log.info(sb.toString());
		}

		StringBuilder sb = new StringBuilder();
		sb.append("table=").append(tableName);
		sb.append(", count=").append(count);
		if (!map.isEmpty()) {
			sb.append(" (");
			for (String cf : map.keySet()) {
				sb.append(cf).append("=").append(map.get(cf)).append(", ");
			}
			sb.delete(sb.length() - 2, sb.length());
			sb.append(")");
		}
		log.info(sb.toString());
		log.info("---------------------count end: " + tableName);
	}

	public byte[][] hashSplitKeys(String startKey, String endKey, int numRegions) {
		byte[] sbytes = ValueUtils.parseIntBytes(startKey);
		byte[] ebytes = ValueUtils.parseIntBytes(endKey);
		byte[][] splitKeys = Bytes.split(sbytes, ebytes, numRegions - 1);
		byte[][] result = new byte[splitKeys.length - 2][];
		for (int i = 0; i < result.length; i++) {
			result[i] = splitKeys[i + 1];
		}
		return result;
	}

}
