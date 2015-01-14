package com.run.ayena.store.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.HBaseUtils;
import com.run.ayena.store.util.ObjectUtils;

/**
 * 将合并后的变更对象档案导入HBase库. <br/>
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectImportHBaseMR extends RunTool {
	private static Logger log = LoggerFactory
			.getLogger(ObjectImportHBaseMR.class);

	public static void main(String[] args) throws Exception {
		execMain(new ObjectImportHBaseMR(), args);
	}

	@Override
	public int exec(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		job.setJarByClass(ObjectImportHBaseMR.class);
		job.setMapperClass(HBaseMapper.class);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);
		if (waitForCompletion(job, true)) {
			return 0;
		}
		return -1;
	}

	public static class HBaseMapper extends
			Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

		protected ByteBuffer bb;

		protected HConnection connection;
		protected Durability dura;

		// base htable
		protected HTableInterface baseTable;
		protected byte[] baseFamily = new byte[] { 'f' };
		protected byte[] baseQualifier = new byte[] { 'o' };

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			Configuration hbaseconf = HBaseConfiguration.create();
			hbaseconf.addResource(conf);

			connection = HConnectionManager.createConnection(hbaseconf);
			String durability = conf.get("hbase.durability", "SKIP_WAL");
			dura = HBaseUtils.getDurability(durability);
			boolean autoFlush = conf.getBoolean("hbase.autoFlush", false);
			boolean clearBufferOnFail = conf.getBoolean(
					"hbase.clearBufferOnFail", false);
			long writeBufferSize = conf.getLong("hbase.client.write.buffer",
					1024 * 1024 * 2L);
			log.info("<conf> hbase.durability = {}", durability);
			log.info("<conf> hbase.autoFlush = {}", autoFlush);
			log.info("<conf> hbase.clearBufferOnFail = {}", clearBufferOnFail);
			log.info("<conf> hbase.writeBufferSize = {}", writeBufferSize);
			log.info("<conf> hbase.zookeeper.quorum = {}",
					conf.get("hbase.zookeeper.quorum"));

			baseTable = connection.getTable(HBaseUtils.getObjectTableName(conf,
					"base"));
			baseTable.setAutoFlush(autoFlush, clearBufferOnFail);
			baseTable.setWriteBufferSize(writeBufferSize);
			log.info("getTable ok #" + baseTable.getName());

			int capacity = conf.getInt("oid.buffersize",
					ObjectUtils.DEFAULT_OID_BUFFERSIZE);
			bb = ByteBuffer.allocateDirect(capacity);

			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (baseTable != null) {
				baseTable.close();
			}
			if (connection != null) {
				connection.close();
			}

			log.info("cleanup ok.");
		}

		@Override
		protected void map(BytesWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			ObjectData.ObjectBase ob = ObjectData.ObjectBase.PARSER.parseFrom(
					value.getBytes(), 0, value.getLength());

			bb.clear();
			bb.put(Bytes.head(key.getBytes(), 2));
			bb.put(Bytes.toBytes(ob.getType()));
			bb.put(ObjectUtils.sepByte);
			bb.put(Bytes.toBytes(ob.getOid()));
			bb.put(ObjectUtils.sepByte);
			bb.put(Bytes.toBytes(ob.getDataSource()));
			bb.put(ObjectUtils.sepByte);
			bb.put(Bytes.toBytes(ob.getProtocol()));
			bb.put(ObjectUtils.sepByte);
			bb.put(Bytes.toBytes(ob.getAction()));
			bb.flip();

			byte[] row = Bytes.toBytes(bb);
			byte[] valueBytes = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, valueBytes, 0,
					value.getLength());

			Put put = new Put(row, System.currentTimeMillis());
			put.setDurability(dura);
			put.add(baseFamily, baseQualifier, valueBytes);
			baseTable.put(put);
		}

	}

}
