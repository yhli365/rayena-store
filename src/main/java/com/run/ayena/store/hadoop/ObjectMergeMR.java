package com.run.ayena.store.hadoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Parser;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.object.TObjectBase;

/**
 * 基于HDFS文件存储的对象归并操作: 定期批量处理. <br/>
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectMergeMR extends RunTool {
	private static Logger log = LoggerFactory.getLogger(ObjectMergeMR.class);
	private static final String MOSNAME_ORIG = "oborig";

	public static void main(String[] args) throws Exception {
		execMain(new ObjectMergeMR(), args);
	}

	@Override
	public int exec(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		job.setJarByClass(ObjectMergeMR.class);
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

		protected MultipleOutputs<BytesWritable, BytesWritable> mos;
		protected BytesWritable outValue = new BytesWritable();

		protected Parser<ObjectData.ObjectBase> parser;
		protected HashMap<String, TObjectBase> dataMap = new HashMap<String, TObjectBase>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<BytesWritable, BytesWritable>(context);
			parser = ObjectData.ObjectBase.PARSER;
			initCounters(context);
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			log.info("cleanup ok.");
		}

		@Override
		protected void reduce(BytesWritable key,
				Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			dataMap.clear();
			ObjectData.ObjectBase ob = null;
			for (BytesWritable value : values) {
				ob = parser.parseFrom(value.getBytes(), 0, value.getLength());

				String tid = TObjectBase.getDimId(ob);
				TObjectBase tob = dataMap.get(tid);
				if (tob == null) {
					tob = new TObjectBase(ob);
					dataMap.put(tid, tob);
				}
				tob.merge(ob);
			}

			String otype = ob.getType();
			String oid = ob.getOid();
			for (TObjectBase tob : dataMap.values()) {
				if (tob.isOasChanged()) {
					byte[] valueBytes = tob.toObjectBaseBytes(otype, oid);
					outValue.set(valueBytes, 0, valueBytes.length);
					context.write(key, outValue);

					if (tob.isOasStored()) {
						context.getCounter(ObjectCounter.BASE_NUM_UPDATE)
								.increment(1);
					} else {
						context.getCounter(ObjectCounter.BASE_NUM_NEW)
								.increment(1);
					}
					context.getCounter(ObjectCounter.BASE_BYTES_WRITE)
							.increment(valueBytes.length);
				} else {
					byte[] valueBytes = tob.toObjectBaseBytes(otype, oid);
					outValue.set(valueBytes, 0, valueBytes.length);
					mos.write(key, outValue, MOSNAME_ORIG);

					context.getCounter(ObjectCounter.BASE_NUM_UNCHANGED)
							.increment(1);
					context.getCounter(ObjectCounter.BASE_BYTES_UNCHANGED)
							.increment(valueBytes.length);
				}
			}
		}

		private void initCounters(Context context) {
			for (Enum<?> e : ObjectCounter.values()) {
				context.getCounter(e).setValue(0);
			}
		}

	}

}
