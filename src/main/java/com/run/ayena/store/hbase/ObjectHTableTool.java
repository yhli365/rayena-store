package com.run.ayena.store.hbase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.pbf.ObjectStore;
import com.run.ayena.store.util.ObjectPbfUtils;

/**
 * 对象档案归并测试.
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectHTableTool extends Configured implements Tool {
	private static final Logger log = LoggerFactory
			.getLogger(ObjectHTableTool.class);

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ObjectHTableTool(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String cmd = args[0];
		if ("-createTable".equalsIgnoreCase(cmd)) {
			return createTable(conf, args);
		} else if ("-dropTable".equalsIgnoreCase(cmd)) {
			return dropTable(conf, args);
		} else if ("-data".equalsIgnoreCase(cmd)) {
			return data(conf, args);
		} else if ("-load".equalsIgnoreCase(cmd)) {
			return load(conf, args);
		} else if ("-q.info".equalsIgnoreCase(cmd)) {
			return qinfo(conf, args);
		} else if ("-q.base".equalsIgnoreCase(cmd)) {
			return qbase(conf, args);
		} else if ("-delete".equalsIgnoreCase(cmd)) {
			return delete(conf, args);
		} else {
			System.err.println("Unkown command: " + cmd);
		}
		return 0;
	}

	private int dropTable(Configuration conf, String[] args) throws IOException {
		HBaseClient hc = new HBaseClient(conf);
		try {
			TableName tableName = getObjectTableName(conf, "base");
			hc.deleteTable(tableName);

			tableName = getObjectTableName(conf, "info");
			hc.deleteTable(tableName);
			return 0;
		} finally {
			hc.close();
		}
	}

	private int createTable(Configuration conf, String[] args)
			throws IOException {
		HBaseClient hc = new HBaseClient(conf);
		try {
			// base
			TableName tableName = getObjectTableName(conf, "base");
			int numRegions = conf.getInt("numRegions.base", -1);
			if (numRegions < 0) {
				numRegions = conf.getInt("numRegions", 5);
			}
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor fd = new HColumnDescriptor("f");
			fd.setBloomFilterType(BloomType.ROW);
			fd.setCompressionType(Compression.Algorithm.SNAPPY);
			fd.setMaxVersions(1);
			fd.setBlocksize(131072);
			desc.addFamily(fd);
			byte[][] splitKeys = hc.hashSplitKeys("0,0", "255,255", numRegions);
			hc.createTable(desc, splitKeys);

			// info
			tableName = getObjectTableName(conf, "info");
			numRegions = conf.getInt("numRegions.info", -1);
			if (numRegions < 0) {
				numRegions = conf.getInt("numRegions", 5);
			}
			desc = new HTableDescriptor(tableName);
			fd = new HColumnDescriptor("f");
			fd.setBloomFilterType(BloomType.ROWCOL);
			fd.setCompressionType(Compression.Algorithm.SNAPPY);
			fd.setMaxVersions(1);
			fd.setBlocksize(131072);
			desc.addFamily(fd);
			splitKeys = hc.hashSplitKeys("0,0", "255,255", numRegions);
			hc.createTable(desc, splitKeys);
			return 0;
		} finally {
			hc.close();
		}
	}

	private TableName getObjectTableName(Configuration conf, String tableid) {
		String type = conf.get("type", "ren");
		String tablePrefix = conf.get("table.prefix");
		String name = type + tableid;
		if (StringUtils.isNotEmpty(tablePrefix)) {
			name = tablePrefix + name;
		}
		TableName tableName = TableName.valueOf(name);
		return tableName;
	}

	private int data(Configuration conf, String[] args) throws IOException {
		File f = new File(args[1]);
		if (!f.exists() || f.isDirectory()) {
			throw new FileNotFoundException("Input bcp file is not found: "
					+ f.getAbsolutePath());
		}
		log.info("Parse input bcp file: " + f.getAbsolutePath());
		List<ObjectData.ObjectBase> dataList = ObjectPbfUtils
				.parseObjectBaseFromBcpFile(f);
		System.out.println("record size=" + dataList.size());
		return 0;
	}

	private int load(Configuration conf, String[] args) throws IOException {
		File f = new File(args[1]);
		if (!f.exists() || f.isDirectory()) {
			throw new FileNotFoundException("Input bcp file is not found: "
					+ f.getAbsolutePath());
		}
		log.info("Parse input bcp file: " + f.getAbsolutePath());
		List<ObjectData.ObjectBase> dataList = ObjectPbfUtils
				.parseObjectBaseFromBcpFile(f);
		System.out.println("record size=" + dataList.size());

		HTableObjectMerge om = new HTableObjectMerge();
		om.setup(conf);
		try {
			for (ObjectData.ObjectBase ob : dataList) {
				om.merge(ob);
			}
		} finally {
			om.cleanup(conf);
		}
		return 0;
	}

	private int qbase(Configuration conf, String[] args) throws IOException,
			NoSuchAlgorithmException {
		HBaseClient hc = new HBaseClient(conf);
		TableName tableName = getObjectTableName(conf, "base");
		HTableInterface table = hc.getHTable(tableName);
		try {
			byte sep = 1;
			ByteBuffer bb = ByteBuffer.allocate(4096);
			bb.put(Bytes.toBytes(args[1])); // type
			bb.put(sep);
			bb.put(Bytes.toBytes(args[2])); // oid
			bb.flip();
			byte[] bytes = Bytes.toBytes(bb);
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md5 = md.digest(bytes);

			if (args.length > 3) {
				bb.clear();
				bb.put(Bytes.head(md5, 2));
				bb.put(bytes);
				bb.put(sep);
				bb.put(Bytes.toBytes(args[3]));
				bb.flip();
				byte[] row = Bytes.toBytes(bb);

				Get get = new Get(row);
				Result r = table.get(get);
				if (r.isEmpty()) {
					System.out.println("Result is empty!!!");
					return 0;
				}
				Cell c = r.getColumnLatestCell(Bytes.toBytes("f"),
						Bytes.toBytes("o"));
				printCell4StoreBase(c);
			} else {
				byte[] startRow = Bytes.add(Bytes.head(md5, 2), bytes,
						new byte[] { sep });
				byte[] stopRow = Bytes.add(Bytes.head(md5, 2), bytes,
						new byte[] { 2 });

				Scan scan = new Scan(startRow, stopRow);
				ResultScanner rs = table.getScanner(scan);
				Result r = rs.next();
				if (r == null || r.isEmpty()) {
					System.out.println("Result is empty!!!");
					return 0;
				}
				while (r != null && !r.isEmpty()) {
					Cell c = r.getColumnLatestCell(Bytes.toBytes("f"),
							Bytes.toBytes("o"));
					printCell4StoreBase(c);
					r = rs.next();
				}
			}

			return 0;
		} finally {
			table.close();
			hc.close();
		}
	}

	private int qinfo(Configuration conf, String[] args) throws IOException,
			NoSuchAlgorithmException {
		HBaseClient hc = new HBaseClient(conf);
		TableName tableName = getObjectTableName(conf, "info");
		HTableInterface table = hc.getHTable(tableName);
		try {
			byte sep = 1;
			ByteBuffer bb = ByteBuffer.allocate(4096);
			bb.put(Bytes.toBytes(args[1])); // type
			bb.put(sep);
			bb.put(Bytes.toBytes(args[2])); // oid
			bb.flip();
			byte[] bytes = Bytes.toBytes(bb);
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md5 = md.digest(bytes);
			byte[] row = Bytes.add(Bytes.head(md5, 2), bytes);

			Get get = new Get(row);
			Result r = table.get(get);
			if (r.isEmpty()) {
				System.out.println("Result is empty!!!");
				return 0;
			}
			Cell c = r.getColumnLatestCell(Bytes.toBytes("f"),
					Bytes.toBytes("o"));
			printCell4StoreInfo(c);
			return 0;
		} finally {
			table.close();
			hc.close();
		}
	}

	private void printCell4StoreBase(Cell c)
			throws InvalidProtocolBufferException {
		ObjectStore.StoreBase ossi = ObjectStore.StoreBase.PARSER.parseFrom(
				c.getValueArray(), c.getValueOffset(), c.getValueLength());
		String rows = Bytes.toString(c.getRowArray(), c.getRowOffset() + 2,
				c.getRowLength() - 2);
		String[] rowarr = rows.split("\u0001");
		StringBuilder sb = new StringBuilder("-----Object Base:{");
		sb.append("\n type: ").append(rowarr[0]);
		sb.append("\n oid: ").append(rowarr[1]);
		sb.append("\n data_source: ").append(rowarr[2]);
		List<ObjectStore.StoreAttr> attrs = ossi.getPropsList();
		sb.append("\n props: [");
		for (ObjectStore.StoreAttr sa : attrs) {
			sb.append("\n\t{code: ").append(sa.getCode());
			sb.append(", value: ").append(sa.getValue());
			sb.append(", protocol: ").append(sa.getProtocol());
			sb.append(", action: ").append(sa.getAction());
			sb.append(", count: ").append(sa.getCount());
			sb.append(", day_count: ").append(sa.getDayCount());
			sb.append(", first_time: ").append(toDateString(sa.getFirstTime()));
			sb.append(", last_time: ").append(toDateString(sa.getLastTime()));

			SortedMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
			for (int i = 0; i < sa.getDayValuesList().size(); i++) {
				map.put(sa.getDayValuesList().get(i),
						sa.getDayStatsList().get(i));
			}
			sb.append(", day_stats: ").append(map);

			sb.append("},");
		}
		sb.append("\n    ]\n}");
		System.out.println(sb.toString());
	}

	private void printCell4StoreInfo(Cell c)
			throws InvalidProtocolBufferException {
		ObjectStore.StoreInfo ossi = ObjectStore.StoreInfo.PARSER.parseFrom(
				c.getValueArray(), c.getValueOffset(), c.getValueLength());
		String rows = Bytes.toString(c.getRowArray(), c.getRowOffset() + 2,
				c.getRowLength() - 2);
		String[] rowarr = rows.split("\u0001");
		StringBuilder sb = new StringBuilder("-----Object Info:{");
		sb.append("\n type: ").append(rowarr[0]);
		sb.append("\n oid: ").append(rowarr[1]);
		sb.append("\n count: ").append(ossi.getCount());
		sb.append("\n day_Count: ").append(ossi.getDayCount());
		sb.append("\n first_time: ").append(toDateString(ossi.getFirstTime()));
		sb.append("\n last_time: ").append(toDateString(ossi.getLastTime()));
		SortedMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
		for (int i = 0; i < ossi.getDayValuesList().size(); i++) {
			map.put(ossi.getDayValuesList().get(i),
					ossi.getDayStatsList().get(i));
		}
		sb.append("\n day_stats: ").append(map);
		List<ObjectStore.StoreAttr> attrs = ossi.getPropsList();
		sb.append("\n props: [");
		for (ObjectStore.StoreAttr sa : attrs) {
			sb.append("\n     {code: ").append(sa.getCode());
			sb.append(", value: ").append(sa.getValue());
			sb.append(", last_time: ").append(toDateString(sa.getLastTime()));
			sb.append("},");
		}
		sb.append("\n    ]\n}");
		System.out.println(sb.toString());
	}

	private String toDateString(int seconds) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return df.format(new Date(seconds * 1000L));
	}

	private int delete(Configuration conf, String[] args) throws IOException,
			NoSuchAlgorithmException {
		HBaseClient hc = new HBaseClient(conf);
		try {
			if (conf.getBoolean("del.base", true)) {
				TableName tableName = getObjectTableName(conf, "base");
				HTableInterface table = hc.getHTable(tableName);

				byte sep = 1;
				ByteBuffer bb = ByteBuffer.allocate(4096);
				bb.put(Bytes.toBytes(args[1])); // type
				bb.put(sep);
				bb.put(Bytes.toBytes(args[2])); // oid
				bb.flip();
				byte[] bytes = Bytes.toBytes(bb);
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] md5 = md.digest(bytes);

				byte[] startRow = Bytes.add(Bytes.head(md5, 2), bytes,
						new byte[] { sep });
				byte[] stopRow = Bytes.add(Bytes.head(md5, 2), bytes,
						new byte[] { 2 });

				Scan scan = new Scan(startRow, stopRow);
				ResultScanner rs = table.getScanner(scan);
				Result r = rs.next();
				if (r == null || r.isEmpty()) {
					System.out.println("Result is empty!!!");
					return 0;
				}
				List<Delete> deletes = new ArrayList<Delete>();
				while (r != null && !r.isEmpty()) {
					Delete del = new Delete(r.getRow());
					deletes.add(del);
					r = rs.next();
				}
				if (!deletes.isEmpty()) {
					table.delete(deletes);
				}
				table.close();
			}

			if (conf.getBoolean("del.info", true)) {
				TableName tableName = getObjectTableName(conf, "info");
				HTableInterface table = hc.getHTable(tableName);

				byte sep = 1;
				ByteBuffer bb = ByteBuffer.allocate(4096);
				bb.put(Bytes.toBytes(args[1])); // type
				bb.put(sep);
				bb.put(Bytes.toBytes(args[2])); // oid
				bb.flip();
				byte[] bytes = Bytes.toBytes(bb);
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] md5 = md.digest(bytes);

				byte[] row = Bytes.add(Bytes.head(md5, 2), bytes);
				Delete del = new Delete(row);
				table.delete(del);
				table.close();
			}
		} finally {
			hc.close();
		}
		return 0;
	}

}
