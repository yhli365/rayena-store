package com.run.ayena.store.solr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.MetaUtils;
import com.run.ayena.store.util.ValueUtils;

/**
 * 生成全文索引json格式数据.
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectIndexProcessor {
	protected static final Logger log = LoggerFactory
			.getLogger(ObjectIndexProcessor.class);

	protected Map<String, JSONObject> baseData = new HashMap<String, JSONObject>();
	protected Map<String, JSONObject> infoData = new HashMap<String, JSONObject>();

	protected MessageDigest md;
	protected byte sep = 1;
	protected ByteBuffer bb;
	protected SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	public void setup(Configuration conf) throws IOException {
		int capacity = conf.getInt("buffer.oid.capacity", 2048);
		bb = ByteBuffer.allocateDirect(capacity);
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("MessageDigest.getInstance(MD5) fail", e);
		}
	}

	public void cleanup(Configuration conf) throws IOException {
		File dir = new File(conf.get("solrdata.dir", "solrdata"));
		if (!dir.exists()) {
			dir.mkdirs();
		}

		if (!baseData.isEmpty()) {
			JSONArray jarr = new JSONArray();
			for (JSONObject jo : baseData.values()) {
				jarr.put(jo);
			}
			String fname = "base_" + df.format(new Date());
			File tmpf = new File(dir, fname + ".tmp");
			BufferedWriter bwBase = new BufferedWriter(new FileWriter(tmpf));
			bwBase.write(jarr.toString(2));
			bwBase.close();
			File dstf = new File(dir, fname + ".json");
			tmpf.renameTo(dstf);
			log.info("solr data file create: " + dstf.getAbsolutePath());
		}

		if (!infoData.isEmpty()) {
			JSONArray jarr = new JSONArray();
			for (JSONObject jo : infoData.values()) {
				jarr.put(jo);
			}
			String fname = "info_" + df.format(new Date());
			File tmpf = new File(dir, fname + ".tmp");
			BufferedWriter bwInfo = new BufferedWriter(new FileWriter(tmpf));
			bwInfo.write(jarr.toString(2));
			bwInfo.close();
			File dstf = new File(dir, fname + ".json");
			tmpf.renameTo(dstf);
			log.info("solr data file create: " + dstf.getAbsolutePath());
		}
	}

	public void indexBase(ObjectData.ObjectBase ob) throws IOException {
		bb.clear();
		bb.put(Bytes.toBytes(ob.getType()));
		bb.put(sep);
		bb.put(Bytes.toBytes(ob.getOid()));
		bb.put(sep);
		bb.put(Bytes.toBytes(ob.getDataSource()));
		bb.flip();
		String id = ValueUtils.md5str(Bytes.toBytes(bb));

		// JSON格式数据解析对象
		JSONObject jo = new JSONObject();
		jo.put("id", id);
		jo.put("H010014", System.currentTimeMillis() / 1000);
		jo.put("A010001", ob.getType());
		jo.put("A010002", ob.getOid());
		jo.put("B050016", ob.getDataSource());
		jo.put("H010001", ob.getProtocol());
		jo.put("H010003", ob.getAction());

		JSONArray jarr = new JSONArray();
		for (ObjectData.ObjectAttr attr : ob.getPropsList()) {
			JSONObject joa = new JSONObject();
			joa.put(attr.getCode(), attr.getValue());
			jarr.put(joa);
		}
		jo.put("props", jarr);

		baseData.put(id, jo);
	}

	public void indexInfo(ObjectData.ObjectInfo ob) throws IOException {
		bb.clear();
		bb.put(Bytes.toBytes(ob.getType()));
		bb.put(sep);
		bb.put(Bytes.toBytes(ob.getOid()));
		bb.flip();
		String id = ValueUtils.md5str(Bytes.toBytes(bb));

		// JSON格式数据解析对象
		JSONObject jo = new JSONObject();
		jo.put("H010014", System.currentTimeMillis() / 1000);
		jo.put("A010001", ob.getType());
		jo.put("A010002", ob.getOid());

		JSONArray jarr;
		for (ObjectData.ObjectAttr attr : ob.getPropsList()) {
			String code = attr.getCode();
			if (MetaUtils.checkCodeType(code, MetaUtils.VALUE_MULTI)) {
				if (jo.has(code)) {
					jarr = jo.getJSONArray(code);
				} else {
					jarr = new JSONArray();
					jo.put(code, jarr);
				}
				jarr.put(attr.getValue());
			} else {
				jo.put(code, attr.getValue());
			}
		}

		// log.info("Object Info: -----\n{}", jo.toString(2));
		infoData.put(id, jo);
	}

}
