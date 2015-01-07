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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.pbf.ObjectData.ObjectBase;
import com.run.ayena.pbf.ObjectStore;
import com.run.ayena.store.util.ObjectPbfUtils;
import com.run.ayena.store.util.ValueUtils;

/**
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
	private Set<String> propCodes;
	private Set<String> multipropCodes;

	public void setup(Configuration conf) throws IOException {

		propCodes = ObjectPbfUtils.getPropCodes();
		multipropCodes = ObjectPbfUtils.getMultipropCodes();

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

	public void indexBase(ObjectData.ObjectBase odob, ObjectStore.StoreBase sb)
			throws IOException {
		bb.clear();
		bb.put(Bytes.toBytes(odob.getType()));
		bb.put(sep);
		bb.put(Bytes.toBytes(odob.getOid()));
		bb.put(sep);
		bb.put(Bytes.toBytes(odob.getDataSource()));
		bb.flip();
		String id = ValueUtils.md5str(Bytes.toBytes(bb));

		// JSON格式数据解析对象
		JSONObject jo = new JSONObject();
		jo.put("id", id);
		jo.put("H010014", System.currentTimeMillis() / 1000);
		jo.put("A010001", odob.getType());
		jo.put("A010002", odob.getOid());
		jo.put("B050016", odob.getDataSource());

		JSONArray jarr = new JSONArray();
		for (ObjectStore.StoreAttr attr : sb.getPropsList()) {
			JSONObject joa = new JSONObject();
			joa.put(attr.getCode(), attr.getValue());
			joa.put("protocol", attr.getProtocol());
			joa.put("action", attr.getAction());
			jarr.put(joa);
		}
		jo.put("props", jarr);

		baseData.put(id, jo);
	}

	public void indexInfo(ObjectBase odob, ObjectStore.StoreInfo si)
			throws IOException {
		bb.clear();
		bb.put(Bytes.toBytes(odob.getType()));
		bb.put(sep);
		bb.put(Bytes.toBytes(odob.getOid()));
		bb.flip();
		String id = ValueUtils.md5str(Bytes.toBytes(bb));

		// JSON格式数据解析对象
		JSONObject jo = new JSONObject();
		jo.put("H010014", System.currentTimeMillis() / 1000);
		jo.put("A010001", odob.getType());
		jo.put("A010002", odob.getOid());

		JSONArray jarr;
		for (ObjectStore.StoreAttr attr : si.getPropsList()) {
			String code = attr.getCode();
			if (propCodes.contains(code)) {
				jo.put(code, attr.getValue());
			} else if (multipropCodes.contains(code)) {
				if (jo.has(code)) {
					jarr = jo.getJSONArray(code);
				} else {
					jarr = new JSONArray();
					jo.put(code, jarr);
				}
				jarr.put(attr.getValue());
			}
		}

		// log.info("Object Info: -----\n{}", jo.toString(2));
		infoData.put(id, jo);
	}

}
