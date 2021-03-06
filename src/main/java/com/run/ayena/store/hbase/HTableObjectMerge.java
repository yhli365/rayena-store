package com.run.ayena.store.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.hadoop.ObjectCounter;
import com.run.ayena.store.solr.ObjectIndexProcessor;
import com.run.ayena.store.util.HBaseUtils;
import com.run.ayena.store.util.MRUtils;
import com.run.ayena.store.util.MetaUtils;
import com.run.ayena.store.util.ObjectUtils;

/**
 * 基于HBase存储的对象归并操作: 实时流式处理. <br/>
 * 处理流程: 先读取HBase中档案数据(Get)，然后和临时对象归并后再写入HBase库(Put).<br/>
 * 注意事项: 不能关闭HBase自动提交模式，否则客户端缓存数据不能被实时读取，会出现归并错误.<br/>
 * 
 * @author Yanhong Lee
 * 
 */
public class HTableObjectMerge {
	protected static final Logger log = LoggerFactory
			.getLogger(HTableObjectMerge.class);

	protected Configuration conf;

	protected ByteBuffer bb;
	protected MessageDigest md;

	protected HConnection connection;
	protected Durability dura;

	// base htable
	protected HTableInterface baseTable;
	protected byte[] baseFamily = new byte[] { 'f' };
	protected byte[] baseQualifier = new byte[] { 'o' };

	// info htable
	protected boolean enabledInfo;
	protected HTableInterface infoTable;
	protected byte[] infoFamily = new byte[] { 'f' };
	protected byte[] infoQualifier = new byte[] { 'o' };

	// protobuf
	protected List<ObjectData.ObjectAttr> sbbAttrs = new ArrayList<ObjectData.ObjectAttr>(
			1000);
	protected ObjectData.ObjectBase.Builder sbb = ObjectData.ObjectBase
			.newBuilder();
	protected ObjectData.ObjectInfo.Builder sib = ObjectData.ObjectInfo
			.newBuilder();
	protected ObjectData.ObjectAttr.Builder saBuilder = ObjectData.ObjectAttr
			.newBuilder();

	protected ObjectIndexProcessor indexProcessor;

	public void setup(Configuration userConf) throws IOException {
		conf = HBaseConfiguration.create();
		conf.addResource(userConf);

		connection = HConnectionManager.createConnection(conf);
		String durability = userConf.get("hbase.durability", "SKIP_WAL");
		dura = HBaseUtils.getDurability(durability);
		// boolean autoFlush = userConf.getBoolean("hbase.autoFlush", false);
		// boolean clearBufferOnFail = userConf.getBoolean(
		// "hbase.clearBufferOnFail", false);
		// long writeBufferSize = userConf.getLong("hbase.client.write.buffer",
		// 1024 * 1024 * 4L);
		log.info("<conf> hbase.durability = {}", durability);
		// log.info("<conf> hbase.autoFlush = {}", autoFlush);
		// log.info("<conf> hbase.clearBufferOnFail = {}", clearBufferOnFail);
		// log.info("<conf> hbase.writeBufferSize = {}", writeBufferSize);
		log.info("<conf> hbase.zookeeper.quorum = {}",
				conf.get("hbase.zookeeper.quorum"));

		baseTable = connection.getTable(HBaseUtils.getObjectTableName(conf,
				"base"));
		// baseTable.setAutoFlush(autoFlush, clearBufferOnFail);
		// baseTable.setWriteBufferSize(writeBufferSize);
		log.info("getTable ok #" + baseTable.getName());

		int capacity = conf.getInt("oid.buffersize",
				ObjectUtils.DEFAULT_OID_BUFFERSIZE);
		bb = ByteBuffer.allocateDirect(capacity);
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("MessageDigest.getInstance(MD5) fail", e);
		}

		enabledInfo = conf.getBoolean("htable.info.enabled", true);
		log.info("<conf> htable.info.enabled = {}", enabledInfo);
		if (enabledInfo) {
			infoTable = connection.getTable(HBaseUtils.getObjectTableName(conf,
					"info"));
			// infoTable.setAutoFlush(autoFlush, clearBufferOnFail);
			// infoTable.setWriteBufferSize(writeBufferSize);
			log.info("getTable ok #" + infoTable.getName());
		}

		boolean flag = conf.getBoolean("index.enabled", false);
		log.info("<conf> index.enabled = {}", flag);
		if (flag) {
			indexProcessor = new ObjectIndexProcessor();
			indexProcessor.setup(conf);
		}

		MRUtils.initCounters(ObjectCounter.values());
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
		if (indexProcessor != null) {
			indexProcessor.cleanup(conf);
		}
		log.info("cleanup ok.");
	}

	/**
	 * 对象档案归并: 实时流式处理.
	 * 
	 * @param ob
	 *            临时对象
	 * @throws IOException
	 */
	public void merge(ObjectData.ObjectBase ob) throws IOException {
		int ts = ob.getCaptureTime();
		int dayValue = ObjectUtils.dayValue(ts);

		bb.clear();
		bb.put(Bytes.toBytes(ob.getType()));
		bb.put(ObjectUtils.sepByte);
		bb.put(Bytes.toBytes(ob.getOid()));

		int len = bb.position();
		byte[] infoRow = new byte[len + 2];
		bb.rewind();
		bb.get(infoRow, 2, len);

		md.update(infoRow, 2, len);
		byte[] md5 = md.digest();
		System.arraycopy(md5, 0, infoRow, 0, 2);

		bb.put(ObjectUtils.sepByte);
		bb.put(Bytes.toBytes(ob.getDataSource()));
		bb.put(ObjectUtils.sepByte);
		bb.put(Bytes.toBytes(ob.getProtocol()));
		bb.put(ObjectUtils.sepByte);
		bb.put(Bytes.toBytes(ob.getAction()));
		len = bb.position();
		byte[] baseRow = new byte[bb.position() + 2];
		System.arraycopy(md5, 0, baseRow, 0, 2);
		bb.rewind();
		bb.get(baseRow, 2, len);

		baseMerge(ob, baseRow, ts, dayValue);
		if (enabledInfo) {
			infoMerge(ob, infoRow, ts, dayValue);
		}
	}

	private void baseMerge(ObjectData.ObjectBase ob, byte[] row, int ts,
			int dayValue) throws IOException {
		// 待归并属性(编码->多值)
		HashMultimap<String, String> props = HashMultimap.create();
		for (ObjectData.ObjectAttr odoa : ob.getPropsList()) {
			props.put(odoa.getCode(), odoa.getValue());
		}

		List<Integer> dayValues = new ArrayList<Integer>();
		List<Integer> dayStats = new ArrayList<Integer>();

		Get get = new Get(row);
		Result r = baseTable.get(get);
		sbbAttrs.clear();
		if (r.isEmpty()) {
			MRUtils.incCounter(ObjectCounter.BASE_NUM_NEW, 1);
		} else {
			MRUtils.incCounter(ObjectCounter.BASE_NUM_UPDATE, 1);
			Cell c = r.getColumnLatestCell(baseFamily, baseQualifier);
			MRUtils.incCounter(ObjectCounter.BASE_BYTES_READ, c.getRowLength()
					+ c.getValueLength());
			ObjectData.ObjectBase ossb = ObjectData.ObjectBase.PARSER
					.parseFrom(c.getValueArray(), c.getValueOffset(),
							c.getValueLength());
			List<ObjectData.ObjectAttr> attrs = ossb.getPropsList();
			for (ObjectData.ObjectAttr sa : attrs) {
				if (props.containsEntry(sa.getCode(), sa.getValue())) {
					// 旧属性值归并统计
					saBuilder.clear();
					saBuilder.setCode(sa.getCode());
					saBuilder.setValue(sa.getValue());
					if (ts > sa.getLastTime()) {
						saBuilder.setLastTime(ts);
					} else {
						saBuilder.setLastTime(sa.getLastTime());
					}
					if (ts < sa.getFirstTime()) {
						saBuilder.setFirstTime(ts);
					} else {
						saBuilder.setFirstTime(sa.getFirstTime());
					}
					saBuilder.setCount(sa.getCount() + 1);
					// 统计天数
					dayValues.clear();
					dayValues.addAll(sa.getDayValuesList());
					dayStats.clear();
					dayStats.addAll(sa.getDayStatsList());
					int idx = Collections.binarySearch(dayValues, dayValue);
					if (idx < 0) {
						saBuilder.setDayCount(sa.getDayCount() + 1);
						idx = -idx - 1;
						dayValues.add(idx, dayValue);
						dayStats.add(idx, 1);
					} else {
						saBuilder.setDayCount(sa.getDayCount());
						dayStats.set(idx, dayStats.get(idx) + 1);
					}
					saBuilder.addAllDayValues(dayValues);
					saBuilder.addAllDayStats(dayStats);

					sbbAttrs.add(saBuilder.build());
					props.remove(sa.getCode(), sa.getValue()); // 删除已归并的属性值
				} else {
					// 旧属性值保留不变
					sbbAttrs.add(sa);
				}
			}
		}
		if (!props.isEmpty()) { // 新发现的属性值
			for (Map.Entry<String, String> entry : props.entries()) {
				saBuilder.clear();
				saBuilder.setCode(entry.getKey());
				saBuilder.setValue(entry.getValue());
				saBuilder.setFirstTime(ts);
				saBuilder.setLastTime(ts);
				saBuilder.setCount(1);
				// 统计天数
				saBuilder.setDayCount(1);
				saBuilder.addDayValues(dayValue);
				saBuilder.addDayStats(1);

				sbbAttrs.add(saBuilder.build());
			}
		}

		// Put.base
		sbb.clear();
		sbb.setType(ob.getType());
		sbb.setOid(ob.getOid());
		sbb.setDataSource(ob.getDataSource());
		sbb.setProtocol(ob.getProtocol());
		sbb.setAction(ob.getAction());
		sbb.addAllProps(sbbAttrs);
		byte[] valueBytes = sbb.build().toByteArray();

		Put put = new Put(row, System.currentTimeMillis());
		put.setDurability(dura);
		put.add(baseFamily, baseQualifier, valueBytes);
		baseTable.put(put);
		MRUtils.incCounter(ObjectCounter.BASE_BYTES_WRITE, row.length
				+ valueBytes.length);

		if (indexProcessor != null && !props.isEmpty()) {
			indexProcessor.indexBase(sbb.build());
		}
	}

	private void infoMerge(ObjectData.ObjectBase odob, byte[] row, int ts,
			int dayValue) throws IOException {
		// 待归并属性(编码->多值)
		HashMap<String, String> props = new HashMap<String, String>();
		HashMultimap<String, String> propsMulti = HashMultimap.create();
		for (ObjectData.ObjectAttr oa : odob.getPropsList()) {
			String code = oa.getCode();
			if (MetaUtils.checkCodeType(code, MetaUtils.VALUE_MULTI)) {
				propsMulti.put(code, oa.getValue());
			} else {
				props.put(code, oa.getValue());
			}
		}

		sib.clear();
		sbbAttrs.clear();
		Get get = new Get(row);
		Result r = infoTable.get(get);
		if (r.isEmpty()) {
			MRUtils.incCounter(ObjectCounter.INFO_NUM_NEW, 1);
			sib.setCount(1);
			sib.setDayCount(1);
			sib.setFirstTime(ts);
			sib.setLastTime(ts);
			sib.addDayValues(dayValue);
			sib.addDayStats(1);
		} else {
			MRUtils.incCounter(ObjectCounter.INFO_NUM_UPDATE, 1);
			Cell c = r.getColumnLatestCell(baseFamily, baseQualifier);
			ObjectData.ObjectInfo ossi = ObjectData.ObjectInfo.PARSER
					.parseFrom(c.getValueArray(), c.getValueOffset(),
							c.getValueLength());
			MRUtils.incCounter(ObjectCounter.INFO_BYTES_READ, c.getRowLength()
					+ c.getValueLength());
			List<ObjectData.ObjectAttr> attrs = ossi.getPropsList();
			for (ObjectData.ObjectAttr sa : attrs) {
				String val = props.remove(sa.getCode());
				if (val != null) {
					if (ts > sa.getLastTime()) { // 属性更新
						saBuilder.clear();
						saBuilder.setCode(sa.getCode());
						saBuilder.setValue(val);
						saBuilder.setLastTime(ts);
						sbbAttrs.add(saBuilder.build());
					} else { // 旧属性值保留不变
						sbbAttrs.add(sa);
					}
				} else {
					// 旧属性值或多值属性保留不变
					if (propsMulti.remove(sa.getCode(), sa.getValue())) {
						// 修改多值最新时间
						saBuilder.clear();
						saBuilder.setCode(sa.getCode());
						saBuilder.setValue(sa.getValue());
						saBuilder.setLastTime(ts);
						sbbAttrs.add(saBuilder.build());
					} else {
						sbbAttrs.add(sa);
					}
				}
			}

			sib.setCount(ossi.getCount() + 1);
			if (ts < ossi.getFirstTime()) {
				sib.setFirstTime(ts);
			} else {
				sib.setFirstTime(ossi.getFirstTime());
			}
			if (ts > ossi.getLastTime()) {
				sib.setLastTime(ts);
			} else {
				sib.setLastTime(ossi.getLastTime());
			}

			// 统计天数
			List<Integer> dayValues = new ArrayList<Integer>(
					ossi.getDayValuesList());
			List<Integer> dayStats = new ArrayList<Integer>(
					ossi.getDayStatsList());
			int idx = Collections.binarySearch(dayValues, dayValue);
			if (idx < 0) {
				sib.setDayCount(ossi.getDayCount() + 1);
				idx = -idx - 1;
				dayValues.add(idx, dayValue);
				dayStats.add(idx, 1);
			} else {
				sib.setDayCount(ossi.getDayCount());
				dayStats.set(idx, dayStats.get(idx) + 1);
			}
			sib.addAllDayValues(dayValues);
			sib.addAllDayStats(dayStats);
		}
		if (!props.isEmpty()) { // 新发现的属性值
			for (Map.Entry<String, String> entry : props.entrySet()) {
				saBuilder.clear();
				saBuilder.setCode(entry.getKey());
				saBuilder.setValue(entry.getValue());
				saBuilder.setLastTime(ts);
				sbbAttrs.add(saBuilder.build());
			}
		}
		if (!propsMulti.isEmpty()) { // 新发现的属性值
			for (Map.Entry<String, String> entry : propsMulti.entries()) {
				saBuilder.clear();
				saBuilder.setCode(entry.getKey());
				saBuilder.setValue(entry.getValue());
				saBuilder.setLastTime(ts);
				sbbAttrs.add(saBuilder.build());
			}
		}
		sib.addAllProps(sbbAttrs);
		sib.setType(odob.getType());
		sib.setOid(odob.getOid());

		// Put.info
		byte[] valueBytes = sib.build().toByteArray();
		Put put = new Put(row, System.currentTimeMillis());
		put.setDurability(dura);
		put.add(infoFamily, infoQualifier, valueBytes);
		infoTable.put(put);
		MRUtils.incCounter(ObjectCounter.INFO_BYTES_WRITE, row.length
				+ valueBytes.length);

		if (indexProcessor != null
				&& (!props.isEmpty() || !propsMulti.isEmpty())) {
			indexProcessor.indexInfo(sib.build());
		}
	}

}
