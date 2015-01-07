package com.run.ayena.store.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

	private static Set<String> propCodes = new HashSet<String>();
	private static Set<String> multipropCodes = new HashSet<String>();

	public static Set<String> getPropCodes() {
		return propCodes;
	}

	public static Set<String> getMultipropCodes() {
		return multipropCodes;
	}

	public static List<ObjectData.ObjectBase> parseObjectBaseFromBcpFile(File f)
			throws IOException {
		propCodes.clear();
		multipropCodes.clear();
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
			for (int i = 0; i < colMetas.length; i++) {
				if (colMetas[i].startsWith("-")) {
					propCodes.add(colMetas[i].substring(1));
				} else if (colMetas[i].startsWith("+")) {
					multipropCodes.add(colMetas[i].substring(1));
				}
			}
			while ((line = br.readLine()) != null) {
				if (line.startsWith("#") || line.isEmpty()) {
					continue;
				}
				String[] colValues = line.split("\t");
				if (colValues.length != colMetas.length) {
					log.warn("error record: " + line);
				}
				ObjectData.ObjectBase.Builder obb = ObjectData.ObjectBase
						.newBuilder();
				Map<String, String> map = new HashMap<String, String>();
				for (int i = 0; i < colMetas.length; i++) {
					if (colValues[i].length() == 0) {
						continue;
					}
					if (colMetas[i].startsWith("-")) {
						ObjectData.ObjectAttr.Builder oab = ObjectData.ObjectAttr
								.newBuilder();
						oab.setCode(colMetas[i].substring(1));
						oab.setValue(colValues[i]);
						obb.addProps(oab);
					} else if (colMetas[i].startsWith("+")) {
						for (String val : colValues[i].split(",")) {
							if (val.trim().length() > 0) {
								ObjectData.ObjectAttr.Builder oab = ObjectData.ObjectAttr
										.newBuilder();
								oab.setCode(colMetas[i].substring(1));
								oab.setValue(val);
								obb.addMultiProps(oab);
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
				// System.out.println(obbo.toString());
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
		StringBuilder sb = new StringBuilder("{");
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
		sb.append("  multiProps: ").append(o.getMultiPropsList().size())
				.append("{");
		for (ObjectData.ObjectAttr oa : o.getMultiPropsList()) {
			sb.append(oa.getCode()).append("=").append(oa.getValue())
					.append(",");
		}
		sb.append("}\n");
		sb.append("}");
		return sb.toString();
	}

}
