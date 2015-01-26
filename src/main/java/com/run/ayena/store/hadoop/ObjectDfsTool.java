package com.run.ayena.store.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.MRUtils;
import com.run.ayena.store.util.ObjectPbfUtils;
import com.run.ayena.store.util.ValueUtils;

/**
 * @author Yanhong Lee
 * 
 */
public class ObjectDfsTool extends Configured implements Tool {
	private static final Logger log = LoggerFactory
			.getLogger(ObjectDfsTool.class);

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ObjectDfsTool(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String cmd = args[0];
		if ("-upload".equalsIgnoreCase(cmd)) {
			return upload(conf, args);
		} else if ("-text".equalsIgnoreCase(cmd)) {
			return text(conf, args);
		} else {
			System.err.println("Unkown command: " + cmd);
		}
		return 0;
	}

	private int text(Configuration conf, String[] args)
			throws UnsupportedEncodingException,
			InvalidProtocolBufferException, IOException {
		SequenceFile.Reader reader = null;
		try {
			BytesWritable key = new BytesWritable();
			BytesWritable value = new BytesWritable();

			SequenceFile.Reader.Option optPath = SequenceFile.Reader
					.file(new Path(args[1]));
			reader = new SequenceFile.Reader(conf, optPath);

			int offset = conf.getInt("offset", 1);
			int len = conf.getInt("len", 10);
			log.info("read data: offset=" + offset + ", len=" + len);

			int seq = 0;
			while (reader.next(key, value)) {
				seq++;
				if (seq < offset) {
					continue;
				}
				if (seq >= offset + len) {
					break;
				}
				String md5 = ValueUtils.toHexString(key.getBytes(), 0,
						key.getLength());
				ObjectData.ObjectBase ob = ObjectData.ObjectBase.PARSER
						.parseFrom(value.getBytes(), 0, value.getLength());
				System.out.println("md5 = " + md5 + ", seq = " + seq);
				System.out.println(ObjectPbfUtils.printToString(ob));
			}
		} finally {
			if (reader != null) {
				IOUtils.closeStream(reader);
			}
		}

		return 0;
	}

	/**
	 * 读取BCP文件数据以protobuf形式上传到HDFS.
	 * 
	 * @param conf
	 * @param args
	 * @return
	 * @throws IOException
	 */
	private int upload(Configuration conf, String[] args) throws IOException {
		File f = new File(args[1]);
		if (!f.exists() || f.isDirectory()) {
			throw new FileNotFoundException("Input bcp file is not found: "
					+ f.getAbsolutePath());
		}
		log.info("Parse input bcp file: " + f.getAbsolutePath());
		List<ObjectData.ObjectBase> dataList = ObjectPbfUtils
				.parseObjectBaseFromBcpFile(f);
		System.out.println("record size=" + dataList.size());

		FileSystem fs = FileSystem.get(conf);
		Path outdir = new Path(args[2]);
		fs.mkdirs(outdir);
		String dstname = f.getName();
		int idx = dstname.lastIndexOf(".");
		if (idx != -1) {
			dstname = dstname.substring(0, idx);
		}
		Path dst = new Path(outdir, dstname + ".tmp");

		CompressionType compress = MRUtils.getCompressionType(conf);
		CompressionCodec codec = MRUtils.getCompressionCodec(conf);
		SequenceFile.Writer writer = null;
		try {
			int count = 0;
			BytesWritable key = new BytesWritable();
			BytesWritable val = new BytesWritable();

			SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(dst);
			SequenceFile.Writer.Option optKey = SequenceFile.Writer
					.keyClass(BytesWritable.class);
			SequenceFile.Writer.Option optVal = SequenceFile.Writer
					.valueClass(BytesWritable.class);
			SequenceFile.Writer.Option optCompress = SequenceFile.Writer
					.compression(compress, codec);
			writer = SequenceFile.createWriter(conf, optPath, optKey, optVal,
					optCompress);
			for (ObjectData.ObjectBase ob : dataList) {
				byte[] bytes = ob.toByteArray();
				val.set(bytes, 0, bytes.length);
				bytes = ObjectPbfUtils.md5(ob);
				key.set(bytes, 0, bytes.length);
				writer.append(key, val);
				count++;
			}
			IOUtils.closeStream(writer);
			writer = null;
			Path dst2 = new Path(outdir, dstname + ".pb"
					+ codec.getDefaultExtension());
			fs.rename(dst, dst2);
			System.out.println("put file ok: count=" + count + " " + args[1]
					+ " => " + dst2.toString());
		} finally {
			if (writer != null) {
				IOUtils.closeStream(writer);
			}
		}

		return 0;
	}

}
