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

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			om = new HTableObjectMerge();
			om.setup(context.getConfiguration());
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			om.cleanup(context.getConfiguration());
			log.info("cleanup ok.");
		}

		@Override
		protected void map(BytesWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			ObjectData.ObjectBase ob = ObjectData.ObjectBase.PARSER.parseFrom(
					value.getBytes(), 0, value.getLength());
			om.merge(ob);
		}

	}

}
