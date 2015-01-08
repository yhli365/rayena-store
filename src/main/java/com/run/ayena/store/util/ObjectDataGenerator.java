package com.run.ayena.store.util;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;

/**
 * @author Yanhong Lee
 * 
 */
public class ObjectDataGenerator {
	private static final Logger log = LoggerFactory
			.getLogger(ObjectDataGenerator.class);

	protected ObjectData.ObjectBase.Builder obb = ObjectData.ObjectBase
			.newBuilder();
	protected ObjectData.ObjectAttr.Builder oab = ObjectData.ObjectAttr
			.newBuilder();

	protected int dayStart;
	protected int daySeconds = 60 * 60 * 24;
	protected int idStart;
	protected int idNum;

	protected String[] types;
	protected String[] dataSources;
	protected String[] protocols;
	protected String[] actions;

	protected String[] propsCodes;
	protected String[] multipropsCodes;
	protected String[] propsSuffixValues;
	protected String[] multipropsSuffixValues;

	protected Random rand = new Random();
	protected MessageDigest md;

	public void setup(Configuration conf) throws IOException {
		String file = conf.get("odg.file", "conf/odg.xml");
		conf.addResource(file);
		log.info("<conf> file = {}", file);

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		Date date = null;
		String str = conf.get("date");
		if (StringUtils.isNotEmpty(str)) {
			try {
				date = df.parse(str);
			} catch (ParseException e) {
				throw new IOException("parse date error: " + str, e);
			}
		} else {
			date = new Date();
		}

		Calendar cdar = Calendar.getInstance();
		cdar.setTime(date);
		cdar.set(Calendar.HOUR_OF_DAY, 0);
		cdar.set(Calendar.MINUTE, 0);
		cdar.set(Calendar.SECOND, 0);
		cdar.set(Calendar.MILLISECOND, 0);
		dayStart = (int) (cdar.getTimeInMillis() / 1000);
		log.info("<conf> date = {}", df.format(date));

		idStart = conf.getInt("id.start", 123456);
		log.info("<conf> id.start = {}", idStart);
		idNum = conf.getInt("id.num", 100000000);
		log.info("<conf> id.num = {}", idNum);

		types = conf.getStrings("A010001");
		log.info("<conf> types = {}", conf.get("A010001"));

		dataSources = conf.getStrings("B050016");
		log.info("<conf> dataSources = {}", conf.get("B050016"));

		protocols = conf.getStrings("H010001");
		log.info("<conf> protocols = {}", conf.get("H010001"));

		actions = conf.getStrings("H010003");
		log.info("<conf> actions = {}", conf.get("H010003"));

		propsCodes = conf.getStrings("props.codes");
		log.info("<conf> props.codes = {}", conf.get("props.codes"));

		multipropsCodes = conf.getStrings("multiprops.codes");
		log.info("<conf> multiprops.codes = {}", conf.get("multiprops.codes"));

		propsSuffixValues = conf.getStrings("props.suffix.values");
		log.info("<conf> props.suffix.values = {}",
				conf.get("props.suffix.values"));

		multipropsSuffixValues = conf.getStrings("multiprops.suffix.values");
		log.info("<conf> multiprops.suffix.values = {}",
				conf.get("multiprops.suffix.values"));

		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("MessageDigest.getInstance(MD5) fail", e);
		}
		log.info("setup ok.");
	}

	public void cleanup(Configuration conf) throws IOException {
		log.info("cleanup ok.");
	}

	public ObjectData.ObjectBase genBase() throws IOException {
		String oid = String.valueOf(idStart + rand.nextInt(idNum));
		String type = types[rand.nextInt(types.length)];
		String dataSource = dataSources[rand.nextInt(dataSources.length)];
		String protocol = protocols[rand.nextInt(protocols.length)];
		String action = actions[rand.nextInt(actions.length)];
		byte[] bytes = Bytes.add(Bytes.toBytes(type), Bytes.toBytes(oid));

		obb.clear();
		obb.setType(type);
		obb.setOid(oid);
		obb.setCaptureTime(dayStart + rand.nextInt(daySeconds));
		obb.setDataSource(dataSource);
		obb.setProtocol(protocol);
		obb.setAction(action);
		for (String code : propsCodes) {
			String value = propsSuffixValues[rand
					.nextInt(propsSuffixValues.length)];

			md.reset();
			md.update(bytes);
			md.update(Bytes.toBytes(code));
			byte[] md5 = md.digest();
			String md5s = ValueUtils.toHexString(md5);

			oab.clear();
			oab.setCode(code);
			oab.setValue(md5s + value);
			obb.addProps(oab.build());
		}
		for (String code : multipropsCodes) {
			String value = multipropsSuffixValues[rand
					.nextInt(multipropsSuffixValues.length)];

			md.reset();
			md.update(bytes);
			md.update(Bytes.toBytes(code));
			byte[] md5 = md.digest();
			String md5s = ValueUtils.toHexString(md5);

			oab.clear();
			oab.setCode(code);
			oab.setValue(md5s + value);
			obb.addProps(oab.build());
		}
		return obb.build();
	}

}
