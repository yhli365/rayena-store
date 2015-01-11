package com.run.ayena.store.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.ObjectPbfUtils;

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
		} else {
			System.err.println("Unkown command: " + cmd);
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

		String str = conf.get("compression.type",
				CompressionType.RECORD.toString());
		CompressionType compress = CompressionType.valueOf(str);
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
		CompressionCodec codec = codecFactory.getCodecByName(conf.get("codec",
				"SnappyCodec")); // LzoCodec,SnappyCodec
		if (codec == null) {
			codec = new DefaultCodec();
		}
		SequenceFile.Writer writer = null;
		try {
			int count = 0;
			BytesWritable key = new BytesWritable();
			BytesWritable val = new BytesWritable();

			Option optPath = SequenceFile.Writer.file(dst);
			Option optKey = SequenceFile.Writer.keyClass(BytesWritable.class);
			Option optVal = SequenceFile.Writer.valueClass(BytesWritable.class);
			Option optCompress = SequenceFile.Writer.compression(compress,
					codec);
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
