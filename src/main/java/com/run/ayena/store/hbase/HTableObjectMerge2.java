package com.run.ayena.store.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

import com.run.ayena.pbf.ObjectData.ObjectBase;
import com.run.ayena.store.hadoop.ObjectCounter;
import com.run.ayena.store.object.TObjectBase;
import com.run.ayena.store.solr.ObjectIndexProcessor;
import com.run.ayena.store.util.HBaseUtils;
import com.run.ayena.store.util.MRUtils;
import com.run.ayena.store.util.ObjectUtils;

/**
 * 基于HBase存储的对象归并操作: 定期批量处理. <br/>
 * 处理流程: 假设新增临时对象和已归并到HBase库的对象档案数据都保存在HDFS上，定期(每日凌晨)进行批量归并.<br/>
 * 注意事项: 归并时不再读取HBase数据以提高性能.<br/>
 * 
 * @author Yanhong Lee
 * 
 */
public class HTableObjectMerge2 {
	protected static final Logger log = LoggerFactory
			.getLogger(HTableObjectMerge2.class);

	protected HashMap<String, TObjectBase> dataMap = new HashMap<String, TObjectBase>();

	protected List<byte[]> changedResults = new ArrayList<byte[]>(100);
	protected List<byte[]> unchangedResults = new ArrayList<byte[]>(100);

	protected Configuration conf;
	protected ByteBuffer bb;

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

		int capacity = conf.getInt("oid.buffersize",
				ObjectUtils.DEFAULT_OID_BUFFERSIZE);
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
		log.info("cleanup ok.");
	}

	/**
	 * 对象档案归并: 定期批量处理.
	 * 
	 * @param key
	 *            对象md5字节码: md5(type+sep+oid)
	 * @param items
	 *            新增临时对象和已存储的归并档案
	 * @throws IOException
	 */
	public void merge(BytesWritable key, List<ObjectBase> items)
			throws IOException {
		ObjectBase ob1 = items.get(0);
		String otype = ob1.getType();
		String oid = ob1.getOid();
		dataMap.clear();
		for (ObjectBase ob : items) {
			String tid = TObjectBase.getDimId(ob);
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
			if (tob.isOasChanged()) {
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

				byte[] valueBytes = tob.toObjectBaseBytes(otype, oid);
				changedResults.add(valueBytes);

				byte[] row = Bytes.toBytes(bb);
				Put put = new Put(row, System.currentTimeMillis());
				put.setDurability(dura);
				put.add(baseFamily, baseQualifier, valueBytes);
				puts.add(put);

				if (tob.isOasStored()) {
					MRUtils.incCounter(ObjectCounter.BASE_NUM_UPDATE, 1);
				} else {
					MRUtils.incCounter(ObjectCounter.BASE_NUM_NEW, 1);
				}
				MRUtils.incCounter(ObjectCounter.BASE_BYTES_WRITE,
						valueBytes.length);
			} else {
				byte[] valueBytes = tob.toObjectBaseBytes(otype, oid);
				unchangedResults.add(valueBytes);
				MRUtils.incCounter(ObjectCounter.BASE_NUM_UNCHANGED, 1);
				MRUtils.incCounter(ObjectCounter.BASE_BYTES_UNCHANGED,
						valueBytes.length);
			}
		}
		if (!puts.isEmpty()) {
			baseTable.put(puts);
		}
	}

	public List<byte[]> getChangedResult() {
		return changedResults;
	}

	public List<byte[]> getUnchangedResult() {
		return unchangedResults;
	}

}
