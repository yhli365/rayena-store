package com.run.ayena.store.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;

/**
 * @author Yanhong Lee
 * 
 */
public class ObjectPbfUtils {
	private static final Logger log = LoggerFactory
			.getLogger(ObjectPbfUtils.class);
	private static MessageDigest md;

	static {
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			log.error("md5 init error", e);
		}
	}

	public static byte[] md5(ObjectData.ObjectBase ob) {
		md.update(Bytes.toBytes(ob.getType()));
		md.update(ObjectUtils.sepByte);
		md.update(Bytes.toBytes(ob.getOid()));
		return md.digest();
	}

	public static List<ObjectData.ObjectBase> parseObjectBaseFromBcpFile(File f)
			throws IOException {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		BufferedReader br = new BufferedReader(new FileReader(f));
		List<ObjectData.ObjectBase> result = new ArrayList<ObjectData.ObjectBase>();
		try {
			int seq = 1;
			String line = br.readLine().trim();
			if (!line.startsWith("@")) {
				throw new IOException("data meta is not config: " + line
						+ ", file=" + f.getAbsolutePath());
			}
			String[] colMetas = line.substring(1).split("\t");

			ObjectData.ObjectBase.Builder obb = ObjectData.ObjectBase
					.newBuilder();
			ObjectData.ObjectAttr.Builder oab = ObjectData.ObjectAttr
					.newBuilder();
			while ((line = br.readLine()) != null) {
				if (line.startsWith("#") || line.isEmpty()) {
					continue;
				}
				String[] colValues = line.split("\t");
				if (colValues.length != colMetas.length) {
					log.warn("error record: " + line);
				}

				obb.clear();
				Map<String, String> map = new HashMap<String, String>();
				for (int i = 0; i < colMetas.length; i++) {
					if (colValues[i].length() == 0) {
						continue;
					}
					String code = colMetas[i].trim();
					int codeType = MetaUtils.getCodeType(code);
					if (codeType == MetaUtils.VALUE_SINGLE) {
						oab.clear();
						oab.setCode(code);
						oab.setValue(colValues[i]);
						obb.addProps(oab.build());
					} else if (codeType == MetaUtils.VALUE_MULTI) {
						for (String val : colValues[i].split(",")) {
							if (val.trim().length() > 0) {
								oab.clear();
								oab.setCode(code);
								oab.setValue(val);
								obb.addProps(oab.build());
							}
						}
					} else {
						map.put(colMetas[i], colValues[i]);
					}
				}
				obb.setType(map.get("A010001"));
				obb.setOid(map.get("A010002"));
				obb.setCaptureTime((int) (df.parse(map.get("H010014"))
						.getTime() / 1000L));
				obb.setDataSource(map.get("B050016"));
				obb.setProtocol(map.get("H010001"));
				obb.setAction(map.get("H010003"));
				ObjectData.ObjectBase ob = obb.build();

				log.info("parse record: " + (seq++));
				System.out.println(line);
				// System.out.println(ob.toString());
				System.out.println(printToString(ob));
				result.add(ob);
			}
			return result;
		} catch (ParseException e) {
			throw new IOException("parse bcp error", e);
		} finally {
			br.close();
		}
	}

	public static String printToString(ObjectData.ObjectBase o) {
		StringBuilder sb = new StringBuilder("ObjectData.ObjectBase-----\n{");
		sb.append(" type: ").append(o.getType()).append("\n");
		sb.append("  oid: ").append(o.getOid()).append("\n");
		sb.append("  capture_time: ").append(o.getCaptureTime()).append("\n");
		sb.append("  data_source: ").append(o.getDataSource()).append("\n");
		sb.append("  protocol: ").append(o.getProtocol()).append("\n");
		sb.append("  action: ").append(o.getAction()).append("\n");
		sb.append("  props: ").append(o.getPropsList().size()).append("{");
		for (ObjectData.ObjectAttr oa : o.getPropsList()) {
			sb.append(oa.getCode()).append("=").append(oa.getValue())
					.append(",");
		}
		sb.append("}\n");
		sb.append("}");
		return sb.toString();
	}

}
