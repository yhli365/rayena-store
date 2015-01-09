package com.run.ayena.store.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.pbf.ObjectData.ObjectBase;
import com.run.ayena.store.object.TObjectAttr;
import com.run.ayena.store.object.TObjectBase;
import com.run.ayena.store.solr.ObjectIndexProcessor;
import com.run.ayena.store.util.HBaseUtils;
import com.run.ayena.store.util.ObjectUtils;

/**
 * 基于HBase存储的对象归并操作.
 * 
 * @author Yanhong Lee
 * 
 */
public class HTableObjectMerge {
	protected static final Logger log = LoggerFactory
			.getLogger(HTableObjectMerge.class);

	protected HashMap<String, TObjectBase> dataMap = new HashMap<String, TObjectBase>();

	protected List<byte[]> changedResults = new ArrayList<byte[]>(100);
	protected List<byte[]> unchangedResults = new ArrayList<byte[]>(100);

	protected Configuration conf;
	protected ByteBuffer bb;

	protected HConnection connection;
	protected Durability dura;

	protected HTableInterface baseTable;
	protected byte[] baseFamily = new byte[] { 'f' };
	protected byte[] baseQualifier = new byte[] { 'o' };

	// info htable
	protected boolean enabledInfo;
	protected HTableInterface infoTable;
	protected byte[] infoFamily = new byte[] { 'f' };
	protected byte[] infoQualifier = new byte[] { 'o' };

	protected ObjectData.ObjectBase.Builder obb = ObjectData.ObjectBase
			.newBuilder();
	protected ObjectData.ObjectAttr.Builder oab = ObjectData.ObjectAttr
			.newBuilder();

	protected ObjectIndexProcessor indexProcessor;

	public void setup(Configuration userConf) throws IOException {
		conf = HBaseConfiguration.create();
		conf.addResource(userConf);

		connection = HConnectionManager.createConnection(conf);
		String durability = userConf.get("hbase.durability", "SKIP_WAL");
		dura = HBaseUtils.getDurability(durability);
		boolean autoFlush = userConf.getBoolean("hbase.autoFlush", false);
		boolean clearBufferOnFail = userConf.getBoolean(
				"hbase.clearBufferOnFail", false);
		long writeBufferSize = userConf.getLong("hbase.client.write.buffer",
				1024 * 1024 * 4L);
		log.info("<conf> hbase.durability = {}", durability);
		log.info("<conf> hbase.autoFlush = {}", autoFlush);
		log.info("<conf> hbase.clearBufferOnFail = {}", clearBufferOnFail);
		log.info("<conf> hbase.writeBufferSize = {}", writeBufferSize);
		log.info("<conf> hbase.zookeeper.quorum = {}",
				conf.get("hbase.zookeeper.quorum"));

		baseTable = connection.getTable(HBaseUtils.getObjectTableName(conf,
				"base"));
		baseTable.setAutoFlush(autoFlush, clearBufferOnFail);
		baseTable.setWriteBufferSize(writeBufferSize);
		log.info("getTable ok #" + baseTable.getName());

		int capacity = conf.getInt("buffer.oid.capacity", 2048);
		bb = ByteBuffer.allocateDirect(capacity);

		enabledInfo = conf.getBoolean("htable.info.enabled", false);
		log.info("<conf> htable.info.enabled = {}", enabledInfo);
		if (enabledInfo) {
			infoTable = connection.getTable(HBaseUtils.getObjectTableName(conf,
					"info"));
			infoTable.setAutoFlush(autoFlush, clearBufferOnFail);
			infoTable.setWriteBufferSize(writeBufferSize);
			log.info("getTable ok #" + infoTable.getName());
		}

		boolean flag = conf.getBoolean("index.enabled", true);
		log.info("<conf> index.enabled = {}", flag);
		if (flag) {
			indexProcessor = new ObjectIndexProcessor();
			indexProcessor.setup(conf);
		}
		log.info("setup ok.");
	}

	public void cleanup(Configuration conf) throws IOException {
		if (baseTable != null) {
			baseTable.close();
		}
		if (infoTable != null) {
			infoTable.close();
		}
		if (connection != null) {
			connection.close();
		}
		log.info("cleanup ok.");
	}

	public void merge(BytesWritable key, List<ObjectBase> items)
			throws IOException {
		ObjectBase ob1 = items.get(0);
		String otype = ob1.getType();
		String oid = ob1.getOid();
		dataMap.clear();
		for (ObjectBase ob : items) {
			String tid = getDimId(ob);
			TObjectBase tob = dataMap.get(tid);
			if (tob == null) {
				tob = new TObjectBase(ob);
				dataMap.put(tid, tob);
			}
			tob.merge(ob);
		}

		this.changedResults.clear();
		this.unchangedResults.clear();
		List<Put> puts = new ArrayList<Put>();
		for (TObjectBase tob : dataMap.values()) {
			if ((tob.getOas() & ObjectUtils.OAS_UNCHANGED) == ObjectUtils.OAS_UNCHANGED) {
				byte[] valueBytes = toObjectBaseBytes(otype, oid, tob);
				unchangedResults.add(valueBytes);

			} else {
				bb.clear();
				bb.put(Bytes.head(key.getBytes(), 2));
				bb.put(Bytes.toBytes(otype));
				bb.put(ObjectUtils.sepByte);
				bb.put(Bytes.toBytes(oid));
				bb.put(ObjectUtils.sepByte);
				bb.put(Bytes.toBytes(tob.getDataSource()));
				bb.put(ObjectUtils.sepByte);
				bb.put(Bytes.toBytes(tob.getProtocol()));
				bb.put(ObjectUtils.sepByte);
				bb.put(Bytes.toBytes(tob.getAction()));
				bb.flip();

				byte[] valueBytes = toObjectBaseBytes(otype, oid, tob);
				changedResults.add(valueBytes);

				byte[] row = Bytes.toBytes(bb);
				Put put = new Put(row, System.currentTimeMillis());
				put.setDurability(dura);
				put.add(baseFamily, baseQualifier, valueBytes);
				puts.add(put);
			}
		}
		if (!puts.isEmpty()) {
			baseTable.put(puts);
		}
	}

	private byte[] toObjectBaseBytes(String otype, String oid, TObjectBase tob) {
		obb.clear();
		obb.setType(otype);
		obb.setOid(oid);
		obb.setDataSource(tob.getDataSource());
		obb.setProtocol(tob.getProtocol());
		obb.setAction(tob.getAction());

		Map<String, Map<String, TObjectAttr>> attrs = tob.getAttrs();
		for (Map.Entry<String, Map<String, TObjectAttr>> e1 : attrs.entrySet()) {
			String code = e1.getKey();
			Map<String, TObjectAttr> values = e1.getValue();
			for (Map.Entry<String, TObjectAttr> e2 : values.entrySet()) {
				String value = e2.getKey();
				TObjectAttr toa = e2.getValue();
				oab.clear();
				oab.setCode(code);
				oab.setValue(value);
				oab.setFirstTime(toa.firstTime);
				oab.setLastTime(toa.lastTime);
				oab.setCount(toa.count);
				oab.setDayCount(toa.dayCount);
				for (Map.Entry<Integer, Integer> de : toa.dayStats.entrySet()) {
					oab.addDayValues(de.getKey());
					oab.addDayStats(de.getValue());
				}
				obb.addProps(oab.build());
			}
		}
		return obb.build().toByteArray();
	}

	public String getDimId(ObjectBase ob) {
		StringBuilder sb = new StringBuilder(ob.getDataSource());
		sb.append(ObjectUtils.sepChar).append(ob.getProtocol());
		sb.append(ObjectUtils.sepChar).append(ob.getAction());
		return sb.toString();
	}

	public List<byte[]> getChangedResult() {
		return changedResults;
	}

	public List<byte[]> getUnchangedResult() {
		return unchangedResults;
	}

}
