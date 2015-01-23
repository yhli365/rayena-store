package com.run.ayena.storm.hdfs;

import org.apache.hadoop.io.SequenceFile;
import org.apache.storm.hdfs.bolt.SequenceFileBolt;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
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
	static final String TOPOLOGY_NAME = "ogpbf-topology";

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
			rotationPolicy = new TimedRotationPolicy(StormUtils.getInt(conf,
					"hdfs.file.rotation.policy.time.interval", 10),
					TimedRotationPolicy.TimeUnit.MINUTES);

		} else { // filesize
			// rotate files when they reach 5MB
			rotationPolicy = new FileSizeRotationPolicy(StormUtils.getInt(conf,
					"hdfs.file.rotation.policy.filesize.length", 5), Units.MB);
		}

		FileNameFormat fileNameFormat = new DateBasedFileNameFormat()
				.withPath("/storm/odg").withPrefix("ren").withExtension(".tmp");

		// create sequence format instance.
		BytesSequenceFormat format = new BytesSequenceFormat("md5", "data");

		SequenceFileBolt bolt = new SequenceFileBolt()
				.withFsUrl(args[0])
				.withFileNameFormat(fileNameFormat)
				.withSequenceFormat(format)
				.withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy)
				.withCompressionType(SequenceFile.CompressionType.BLOCK)
				.withCompressionCodec("snappy")
				.addRotationAction(
						new RenameFileSuffixAction().change(".tmp", ".snappy"));

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SPOUT_ID, spout, 3);
		// SentenceSpout --> MyBolt
		builder.setBolt(BOLT_ID, bolt, 2).shuffleGrouping(SPOUT_ID);

		StormUtils.runTopology(conf, builder);
	}

}
