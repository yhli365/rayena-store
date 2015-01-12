package com.run.ayena.store.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Parser;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.hbase.HTableObjectMerge2;
import com.run.ayena.store.util.MRUtils;

/**
 * 基于HBase存储的对象归并操作: 定期批量处理. <br/>
 * 
 * @author Yanhong Lee
 * 
 */
public class HTableObjectMergeMR2 extends RunTool {
	private static Logger log = LoggerFactory
			.getLogger(HTableObjectMergeMR2.class);

	public static void main(String[] args) throws Exception {
		execMain(new HTableObjectMergeMR2(), args);
	}

	@Override
	public int exec(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		job.setJarByClass(HTableObjectMergeMR2.class);
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(ObjectMergeReducer.class);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileSystem fs = FileSystem.get(conf);

		String str = args[0];
		for (String s : args[0].split(",")) {
			Path p = new Path(s);
			FileStatus[] farr = fs.globStatus(p);
			if (farr == null || farr.length == 0) {
				log.warn("add objectstore path: no files - " + p);
				continue;
			}
			FileInputFormat.addInputPath(job, p);
			log.info("add objectdata path: " + s);
		}
		str = conf.get("objectstore.dir");
		if (StringUtils.isNotEmpty(str)) {
			for (String s : str.split(",")) {
				Path p = new Path(s);
				FileStatus[] farr = fs.globStatus(p);
				if (farr == null || farr.length == 0) {
					log.warn("add objectstore path: no files - " + p);
					continue;
				}
				FileInputFormat.addInputPath(job, p);
				log.info("add objectstore path: " + s);
			}
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(conf.getInt("mapreduce.job.reduces", 10));
		if (waitForCompletion(job, true)) {
			return 0;
		}
		return -1;
	}

	public static class ObjectMergeReducer extends
			Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

		protected Parser<ObjectData.ObjectBase> parser;
		protected List<ObjectData.ObjectBase> items = new ArrayList<ObjectData.ObjectBase>(
				1000);
		protected BytesWritable outValue = new BytesWritable();

		protected HTableObjectMerge2 om = new HTableObjectMerge2();

		private int count;
		private int maxCount;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			parser = ObjectData.ObjectBase.PARSER;
			om.setup(conf);
			maxCount = conf.getInt("counter.max", 1000);
			log.info("<conf> counter.max = {}", maxCount);
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			om.cleanup(conf);
			incCounters(context);
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
			count++;
			if (count > maxCount) {
				incCounters(context);
				count = 0;
			}
		}

		protected void incCounters(Context context) {
			Map<Enum<?>, GenericCounter> map = MRUtils.getCounters();
			for (Map.Entry<Enum<?>, GenericCounter> e : map.entrySet()) {
				context.getCounter(e.getKey()).increment(
						e.getValue().getValue());
			}
			MRUtils.resetCounters();
		}

	}

}
