package com.run.ayena.store.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Parser;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.hbase.HTableObjectMerge;
import com.run.ayena.store.util.MRUtils;

/**
 * @author Yanhong Lee
 * 
 */
public class ObjectMergeHdfsMR extends Configured implements Tool {
	private static Logger log = LoggerFactory
			.getLogger(ObjectMergeHdfsMR.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("mr/ObjectMergeHdfsMR.xml");
		ToolRunner.run(conf, new ObjectMergeHdfsMR(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		MRUtils.initJobConf(conf, this);
		Job job = Job.getInstance(conf);
		job.setJarByClass(ObjectMergeHdfsMR.class);

		FileSystem fs = FileSystem.get(conf);

		String str = args[0];
		for (String s : args[0].split(",")) {
			Path p = new Path(s);
			if (fs.globStatus(p).length == 0) {
				log.warn("add objectstore path: no files - " + p);
				continue;
			}
			FileInputFormat.addInputPath(job, p);
			log.info("add objectdata path: " + s);
		}
		str = conf.get("objectstore.dir", "");
		for (String s : str.split(",")) {
			Path p = new Path(s);
			if (fs.globStatus(p).length == 0) {
				log.warn("add objectstore path: no files - " + p);
				continue;
			}
			FileInputFormat.addInputPath(job, p);
			log.info("add objectstore path: " + s);
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(ObjectMergeReducer.class);

		job.setNumReduceTasks(conf.getInt("mapreduce.job.reduces", 10));
		if (job.waitForCompletion(true)) {
			return 0;
		}
		return -1;
	}

	public static class ObjectMergeReducer extends
			Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

		protected Parser<ObjectData.ObjectBase> parser;
		protected List<ObjectData.ObjectBase> items = new ArrayList<ObjectData.ObjectBase>(
				1000);
		protected BytesWritable outValue;

		protected HTableObjectMerge om = new HTableObjectMerge();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			parser = ObjectData.ObjectBase.PARSER;
			om.setup(conf);
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			om.cleanup(conf);
			log.info("cleanup ok.");
		}

		@Override
		protected void reduce(BytesWritable key,
				Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			items.clear();
			for (BytesWritable value : values) {
				ObjectData.ObjectBase sb = parser.parseFrom(value.getBytes(),
						0, value.getLength());
				items.add(sb);
			}
			om.merge(key, items);
			for (byte[] valueBytes : om.getChangedResult()) {
				outValue.set(valueBytes, 0, valueBytes.length);
				context.write(key, outValue);
			}
			for (byte[] valueBytes : om.getUnchangedResult()) {
				outValue.set(valueBytes, 0, valueBytes.length);
				context.write(key, outValue);
			}
		}
	}

}
