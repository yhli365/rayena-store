package com.run.ayena.store.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
 * @author Yanhong Lee
 * 
 */
public class HTableObjectMergeMR extends Configured implements Tool {
	private static Logger log = LoggerFactory
			.getLogger(HTableObjectMergeMR.class);

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new HTableObjectMergeMR(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.addResource("mr/HTableObjectMergeMR.xml");

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
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			incCounter(context);
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
				incCounter(context);
				count = 0;
			}
		}

		protected void incCounter(Context context) {
			HTableObjectMerge.ObjectStat os = om.getObjectStat();
			context.getCounter(ObjectCounter.NUM_NEW_INFOS).increment(
					os.infoNewNums);
			context.getCounter(ObjectCounter.NUM_UPDATE_INFOS).increment(
					os.infoUptNums);
			context.getCounter(ObjectCounter.NUM_NEW_BASES).increment(
					os.baseNewNums);
			context.getCounter(ObjectCounter.NUM_UPDATE_BASES).increment(
					os.baseUptNums);
			context.getCounter(ObjectCounter.BYTES_READ_INFOS).increment(
					os.infoReadBytes);
			context.getCounter(ObjectCounter.BYTES_WRITE_INFOS).increment(
					os.infoWriteBytes);
			context.getCounter(ObjectCounter.BYTES_READ_BASES).increment(
					os.baseReadBytes);
			context.getCounter(ObjectCounter.BYTES_WRITE_BASES).increment(
					os.baseWriteBytes);

			om.clearObjectStat();
		}
	}

}
