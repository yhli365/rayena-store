package com.run.ayena.store.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import com.run.ayena.pbf.ObjectData.ObjectBase;
import com.run.ayena.pbf.ObjectStore;

/**
 * 基于HBase存储的对象归并操作.
 * 
 * @author Yanhong Lee
 * 
 */
public class HTableObjectMerge {
	protected static final Logger log = LoggerFactory
			.getLogger(HTableObjectMerge.class);

	protected Configuration conf;

	protected HConnection connection;
	protected Durability dura;

	protected byte sep = 1;
	protected ByteBuffer bb;
	protected MessageDigest md;
	protected Calendar cdar = Calendar.getInstance();

	protected HTableInterface baseTable;
	protected byte[] baseFamily = new byte[] { 'f' };
	protected byte[] baseQualifier = new byte[] { 'o' };
	protected ObjectStore.StoreBase.Builder sbb = ObjectStore.StoreBase
			.newBuilder();
	protected List<ObjectStore.StoreAttr> sbbAttrs = new ArrayList<ObjectStore.StoreAttr>(
			1000);

	// info htable
	protected boolean enabledInfo;
	protected HTableInterface infoTable;
	protected byte[] infoFamily = new byte[] { 'f' };
	protected byte[] infoQualifier = new byte[] { 'o' };
	protected ObjectStore.StoreInfo.Builder sib = ObjectStore.StoreInfo
			.newBuilder();

	public void setup(Configuration userConf) throws IOException {
		conf = HBaseConfiguration.create();
		conf.addResource(userConf);

		String type = conf.get("type", "ren");
		String tablePrefix = conf.get("table.prefix");
		if (StringUtils.isNotEmpty(tablePrefix)) {
			tablePrefix = tablePrefix + type;
		} else {
			tablePrefix = type;
		}

		connection = HConnectionManager.createConnection(conf);

		baseTable = connection.getTable(tablePrefix + "base");
		dura = Durability.USE_DEFAULT;

		int capacity = conf.getInt("buffer.oid.capacity", 2048);
		bb = ByteBuffer.allocateDirect(capacity);
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("MessageDigest.getInstance(MD5) fail", e);
		}

		enabledInfo = conf.getBoolean("htable.info.enabled", true);
		if (enabledInfo) {
			infoTable = connection.getTable(tablePrefix + "info");
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

	public void merge(ObjectData.ObjectBase odob) throws IOException {
		int ts = odob.getCaptureTime();
		cdar.setTimeInMillis(ts * 1000L);
		int dayValue = cdar.get(Calendar.YEAR) * 10000
				+ (cdar.get(Calendar.MONTH) + 1) * 100
				+ cdar.get(Calendar.DATE);

		bb.clear();
		bb.put(Bytes.toBytes(odob.getType()));
		bb.put(sep);
		bb.put(Bytes.toBytes(odob.getOid()));

		int len = bb.position();
		byte[] infoRow = new byte[len + 2];
		bb.rewind();
		bb.get(infoRow, 2, len);

		md.update(infoRow, 2, len);
		byte[] md5 = md.digest();
		System.arraycopy(md5, 0, infoRow, 0, 2);

		bb.put(sep);
		bb.put(Bytes.toBytes(odob.getDataSource()));
		len = bb.position();
		byte[] baseRow = new byte[bb.position() + 2];
		System.arraycopy(md5, 0, baseRow, 0, 2);
		bb.rewind();
		bb.get(baseRow, 2, len);

		baseMerge(odob, baseRow, ts, dayValue);
		if (enabledInfo) {
			infoMerge(odob, infoRow, ts, dayValue);
		}
	}

	private void baseMerge(ObjectData.ObjectBase odob, byte[] row, int ts,
			int dayValue) throws IOException {
		String action = odob.getAction();
		String protocol = odob.getProtocol();
		// 待归并属性(编码->多值)
		HashMultimap<String, String> props = HashMultimap.create();
		for (ObjectData.ObjectAttr odoa : odob.getPropsList()) {
			props.put(odoa.getCode(), odoa.getValue());
		}
		for (ObjectData.ObjectAttr odoa : odob.getMultiPropsList()) {
			props.put(odoa.getCode(), odoa.getValue());
		}

		List<Integer> dayValues = new ArrayList<Integer>();
		List<Integer> dayStats = new ArrayList<Integer>();

		Get get = new Get(row);
		Result r = baseTable.get(get);
		sbbAttrs.clear();
		if (!r.isEmpty()) {
			Cell c = r.getColumnLatestCell(baseFamily, baseQualifier);
			ObjectStore.StoreBase ossb = ObjectStore.StoreBase.PARSER
					.parseFrom(c.getValueArray(), c.getValueOffset(),
							c.getValueLength());
			List<ObjectStore.StoreAttr> attrs = ossb.getPropsList();
			for (ObjectStore.StoreAttr sa : attrs) {
				if (StringUtils.equals(sa.getProtocol(), protocol)
						&& StringUtils.equals(sa.getAction(), action)) {
					if (props.containsEntry(sa.getCode(), sa.getValue())) {// 旧属性值归并统计
						ObjectStore.StoreAttr.Builder saBuilder = ObjectStore.StoreAttr
								.newBuilder();
						saBuilder.setCode(sa.getCode());
						saBuilder.setValue(sa.getValue());
						saBuilder.setProtocol(protocol);
						saBuilder.setAction(action);
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
					} else {// 旧属性值保留不变
						sbbAttrs.add(sa);
					}
				} else {// 旧属性值保留不变
					sbbAttrs.add(sa);
				}
			}
		}
		if (!props.isEmpty()) { // 新发现的属性值
			for (Map.Entry<String, String> entry : props.entries()) {
				ObjectStore.StoreAttr.Builder saBuilder = ObjectStore.StoreAttr
						.newBuilder();
				saBuilder.setCode(entry.getKey());
				saBuilder.setValue(entry.getValue());
				saBuilder.setProtocol(protocol);
				saBuilder.setAction(action);
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
		sbb.addAllProps(sbbAttrs);
		Put put = new Put(row, System.currentTimeMillis());
		put.setDurability(dura);
		put.add(baseFamily, baseQualifier, sbb.build().toByteArray());
		baseTable.put(put);
	}

	private void infoMerge(ObjectBase odob, byte[] row, int ts, int dayValue)
			throws IOException {
		// 待归并属性(编码->多值)
		HashMap<String, String> props = new HashMap<String, String>();
		for (ObjectData.ObjectAttr odoa : odob.getPropsList()) {
			props.put(odoa.getCode(), odoa.getValue());
		}
		HashMultimap<String, String> propsMulti = HashMultimap.create();
		for (ObjectData.ObjectAttr odoa : odob.getMultiPropsList()) {
			propsMulti.put(odoa.getCode(), odoa.getValue());
		}

		sib.clear();
		sbbAttrs.clear();
		Get get = new Get(row);
		Result r = infoTable.get(get);
		if (!r.isEmpty()) {
			Cell c = r.getColumnLatestCell(baseFamily, baseQualifier);
			ObjectStore.StoreInfo ossi = ObjectStore.StoreInfo.PARSER
					.parseFrom(c.getValueArray(), c.getValueOffset(),
							c.getValueLength());
			List<ObjectStore.StoreAttr> attrs = ossi.getPropsList();
			for (ObjectStore.StoreAttr sa : attrs) {
				String val = props.remove(sa.getCode());
				if (val != null) {
					if (ts > sa.getLastTime()) { // 属性更新
						ObjectStore.StoreAttr.Builder saBuilder = ObjectStore.StoreAttr
								.newBuilder();
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
						ObjectStore.StoreAttr.Builder saBuilder = ObjectStore.StoreAttr
								.newBuilder();
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
		} else {
			sib.setCount(1);
			sib.setDayCount(1);
			sib.setFirstTime(ts);
			sib.setLastTime(ts);
			sib.addDayValues(dayValue);
			sib.addDayStats(1);
		}
		if (!props.isEmpty()) { // 新发现的属性值
			for (Map.Entry<String, String> entry : props.entrySet()) {
				ObjectStore.StoreAttr.Builder saBuilder = ObjectStore.StoreAttr
						.newBuilder();
				saBuilder.setCode(entry.getKey());
				saBuilder.setValue(entry.getValue());
				saBuilder.setLastTime(ts);
				sbbAttrs.add(saBuilder.build());
			}
		}
		if (!propsMulti.isEmpty()) { // 新发现的属性值
			for (Map.Entry<String, String> entry : propsMulti.entries()) {
				ObjectStore.StoreAttr.Builder saBuilder = ObjectStore.StoreAttr
						.newBuilder();
				saBuilder.setCode(entry.getKey());
				saBuilder.setValue(entry.getValue());
				saBuilder.setLastTime(ts);
				sbbAttrs.add(saBuilder.build());
			}
		}
		sib.addAllProps(sbbAttrs);

		// Put.info
		Put put = new Put(row, System.currentTimeMillis());
		put.setDurability(dura);
		put.add(infoFamily, infoQualifier, sib.build().toByteArray());
		infoTable.put(put);
	}

}