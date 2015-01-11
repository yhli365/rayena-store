package com.run.ayena.store.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.hbase.HTableObjectMerge;
import com.run.ayena.store.util.MRUtils;

/**
 * 基于HBase存储的对象归并操作: 实时流式处理. <br/>
 * 
 * @author Yanhong Lee
 * 
 */
public class HTableObjectMergeMR extends Configured implements Tool {
	private static Logger log = LoggerFactory
			.getLogger(HTableObjectMergeMR.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("mr/HTableObjectMergeMR.xml");
		ToolRunner.run(conf, new HTableObjectMergeMR(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		MRUtils.initJobConf(conf, this);
		Job job = Job.getInstance(conf);
		job.setJarByClass(HTableObjectMergeMR.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HTableObjectMergeMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		if (job.waitForCompletion(true)) {
			return 0;
		}
		return -1;
	}

	public static class HTableObjectMergeMapper extends
			Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

		private HTableObjectMerge om;
		private int count;
		private int maxCount;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			om = new HTableObjectMerge();
			om.setup(conf);
			maxCount = conf.getInt("counter.max", 1000);
			log.info("<conf> counter.max = {}", maxCount);
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			incCounters(context);
			om.cleanup(context.getConfiguration());
			log.info("cleanup ok.");
		}

		@Override
		protected void map(BytesWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			ObjectData.ObjectBase ob = ObjectData.ObjectBase.PARSER.parseFrom(
					value.getBytes(), 0, value.getLength());
			om.merge(ob);
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
