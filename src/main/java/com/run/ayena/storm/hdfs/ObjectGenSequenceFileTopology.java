package com.run.ayena.storm.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.storm.hdfs.bolt.SequenceFileBolt;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

import com.run.ayena.storm.StormUtils;

public class ObjectGenSequenceFileTopology {

	static final String SPOUT_ID = "oggen-spout";
	static final String BOLT_ID = "oghdfs-bolt";
	static final String TOPOLOGY_NAME = "og-topology";

	public static void main(String[] args) throws Exception {

		Config conf = StormUtils.loadConfig(args[0]);

		ObjectGenSpout spout = new ObjectGenSpout();

		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(StormUtils.getInt(conf,
				"hdfs.seqfile.sync.count", 1000));

		// rotate files when they reach 5MB
		String mode = StormUtils.getString(conf, "hdfs.file.rotation.policy",
				"filesize").toLowerCase();
		FileRotationPolicy rotationPolicy;
		if ("time".equals(mode)) {
			// rotate files when 10 minutes
			int interval = StormUtils.getInt(conf,
					"hdfs.file.rotation.policy.time.interval", 600);
			rotationPolicy = new TimedRotationPolicy(interval,
					TimedRotationPolicy.TimeUnit.SECONDS);
		} else { // filesize
			// rotate files when they reach 5MB
			int size = StormUtils.getInt(conf,
					"hdfs.file.rotation.policy.filesize.max", 5);
			rotationPolicy = new FileSizeRotationPolicy(size, Units.MB);
		}

		DateBasedFileNameFormat fileNameFormat = new DateBasedFileNameFormat()
				.withPath(
						StormUtils.getString(conf, "hdfs.file.path",
								"/storm/odg"))
				// path
				.withPrefix(
						StormUtils.getString(conf, "hdfs.file.prefix", "ren"))// prefix
				.withExtension(".tmp");

		// create sequence format instance.
		BytesSequenceFormat format = new BytesSequenceFormat("md5", "data");

		String str = StormUtils.getString(conf, "hdfs.file.compression.type",
				"block");
		CompressionType compress = CompressionType.valueOf(str.toUpperCase());

		String codecStr = StormUtils.getString(conf, "hdfs.file.codec",
				"snappy");// LzoCodec,SnappyCodec
		String extension = StormUtils.getString(conf, "hdfs.file.extension",
				null);
		Configuration hconf = StormUtils.getHdfsConfiguration(conf);
		if (StringUtils.isEmpty(extension)) {
			CompressionCodecFactory codecFactory = new CompressionCodecFactory(
					hconf);
			CompressionCodec codec = codecFactory.getCodecByName(codecStr);
			extension = codec.getDefaultExtension();
		}

		SequenceFileBolt bolt = new SequenceFileBolt()
				.withFsUrl(hconf.get("fs.defaultFS"))
				.withFileNameFormat(fileNameFormat)
				.withSequenceFormat(format)
				.withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy)
				.withCompressionType(compress)
				.withCompressionCodec(codecStr)
				.addRotationAction(
						new RenameFileSuffixAction().change(".tmp", extension));

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SPOUT_ID, spout,
				StormUtils.getInt(conf, "spout.parallel", 3));
		builder.setBolt(BOLT_ID, bolt,
				StormUtils.getInt(conf, "bolt.parallel", 2)).shuffleGrouping(
				SPOUT_ID);

		StormUtils.runTopology(conf, builder);
	}

}
