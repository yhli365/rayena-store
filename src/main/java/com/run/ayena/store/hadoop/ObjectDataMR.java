package com.run.ayena.store.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.MRUtils;
import com.run.ayena.store.util.ObjectDataGenerator;
import com.run.ayena.store.util.ObjectPbfUtils;

/**
 * @author Yanhong Lee
 * 
 */
public class ObjectDataMR extends RunTool {
	private static Logger log = LoggerFactory.getLogger(ObjectDataMR.class);
	private static long MEGA = 0x100000L;;

	public static void main(String[] args) throws Exception {
		execMain(new ObjectDataMR(), args);
	}

	@Override
	public int exec(String[] args) throws Exception {
		Configuration conf = getConf();

		FileSystem fs = FileSystem.get(conf);
		conf.set("data.dir", args[0]);
		createControlFile(conf);
		fs.delete(getWriteDir(conf), true);
		Path dataDir = getDataDir(conf);
		if (fs.exists(dataDir)) {
			throw new IOException("data dir already is exist: " + dataDir);
		}
		fs.mkdirs(dataDir);

		Job job = Job.getInstance(conf);
		job.setJarByClass(ObjectDataMR.class);
		job.setMapperClass(WriteMapper.class);
		job.setReducerClass(AccumulatingReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, getControlDir(conf));
		FileOutputFormat.setOutputPath(job, getWriteDir(conf));

		job.setNumReduceTasks(1);
		long tStart = System.currentTimeMillis();
		if (waitForCompletion(job, true)) {
			long execTime = System.currentTimeMillis() - tStart;
			analyzeResult(conf, fs, execTime, "ObjectGen_results.log",
					getWriteDir(conf));
			return 0;
		}
		return -1;
	}

	private void analyzeResult(Configuration conf, FileSystem fs,
			long execTime, String resFileName, Path resDir) throws IOException {
		long tasks = 0;
		long time = 0;
		Map<String, Object> statMap = new HashMap<String, Object>();
		DataInputStream in = null;
		BufferedReader lines = null;
		try {
			in = new DataInputStream(fs.open(new Path(resDir, "part-r-00000")));
			lines = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = lines.readLine()) != null) {
				// log.info("line: " + line);
				StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
				String attr = tokens.nextToken();
				if (attr.endsWith(":tasks")) {
					tasks = Long.parseLong(tokens.nextToken());
				} else if (attr.endsWith(":time")) {
					time = Long.parseLong(tokens.nextToken());
				} else if (attr
						.startsWith(AccumulatingReducer.VALUE_TYPE_FLOAT)) {
					statMap.put(attr
							.substring(AccumulatingReducer.VALUE_TYPE_FLOAT
									.length()), Float.parseFloat(tokens
							.nextToken()));
				} else if (attr.startsWith(AccumulatingReducer.VALUE_TYPE_LONG)) {
					statMap.put(attr
							.substring(AccumulatingReducer.VALUE_TYPE_LONG
									.length()), Long.parseLong(tokens
							.nextToken()));
				}
			}
		} finally {
			if (in != null) {
				in.close();
			}
			if (lines != null) {
				lines.close();
			}
		}

		List<String> resultLines = new ArrayList<String>();
		resultLines.add("---- ObjectGenIO ---- : " + "write");
		resultLines.add("           Date & time: "
				+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(
						System.currentTimeMillis())));
		resultLines.add("       Number of files: " + tasks);
		resultLines.add("    Test exec time sec: " + (float) execTime / 1000);

		long size = (Long) statMap.get("mb.size");
		float rate = (Float) statMap.get("mb.rate");
		float sqrate = (Float) statMap.get("mb.sqrate");
		double med = rate / 1000 / tasks;
		double stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med * med));
		resultLines.add("---------------------#: by byte");
		resultLines.add("Total MBytes processed: " + toMB(size));
		resultLines.add("     Throughput mb/sec: " + size * 1000.0
				/ (time * MEGA));
		resultLines.add("Average IO rate mb/sec: " + med);
		resultLines.add(" IO rate std deviation: " + stdDev);
		resultLines.add("        Cluster mb/sec: " + size * 1000.0
				/ (execTime * MEGA));

		size = (Long) statMap.get("rec.size");
		rate = (Float) statMap.get("rec.rate");
		sqrate = (Float) statMap.get("rec.sqrate");
		med = rate / 1000 / tasks;
		stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med * med));
		resultLines.add("---------------------#: by record");
		resultLines.add("Total records processed: " + size);
		resultLines.add("     Throughput rec/sec: " + size * 1000.0 / (time));
		resultLines.add("Average IO rate rec/sec: " + med);
		resultLines.add("  IO rate std deviation: " + stdDev);
		resultLines.add("        Cluster rec/sec: " + size * 1000.0
				/ (execTime));

		Path resFile = new Path(resDir, resFileName);
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(fs.create(resFile,
					true)));
			for (String line : resultLines) {
				log.info(line);
				bw.append(line);
				bw.newLine();
			}
		} finally {
			if (bw != null) {
				bw.close();
			}
		}
	}

	private static float toMB(long bytes) {
		return ((float) bytes) / MEGA;
	}

	private static String getBaseDir(Configuration conf) {
		return conf.get("test.build.data", "/benchmarks/ObjectData");
	}

	private static Path getControlDir(Configuration conf) {
		return new Path(
				getBaseDir(conf) + "/" + conf.get(MRJobConfig.JOB_NAME),
				"io_control");
	}

	private static Path getWriteDir(Configuration conf) {
		return new Path(
				getBaseDir(conf) + "/" + conf.get(MRJobConfig.JOB_NAME),
				"io_write");
	}

	private static Path getDataDir(Configuration conf) {
		return new Path(conf.get("data.dir"));
	}

	private void createControlFile(Configuration conf) throws IOException {
		// in records
		long fileSize = conf.getLong("fileSize", 100);
		int nrFiles = conf.getInt("nrFiles", 3);
		log.info("creating control file: " + fileSize + " records, " + nrFiles
				+ " files");

		Path controlDir = getControlDir(conf);
		log.info("control file dir: " + controlDir);

		FileSystem fs = FileSystem.get(conf);
		fs.delete(controlDir, true);
		for (int i = 0; i < nrFiles; i++) {
			String name = "test_objectdata_" + Integer.toString(i);
			Path controlFile = new Path(controlDir, "in_file_" + name);
			SequenceFile.Writer writer = null;
			try {
				Option optPath = SequenceFile.Writer.file(controlFile);
				Option optKey = SequenceFile.Writer.keyClass(Text.class);
				Option optVal = SequenceFile.Writer
						.valueClass(LongWritable.class);
				Option optCompress = SequenceFile.Writer
						.compression(CompressionType.NONE);
				writer = SequenceFile.createWriter(conf, optPath, optKey,
						optVal, optCompress);
				writer.append(new Text(name), new LongWritable(fileSize));
			} catch (Exception e) {
				throw new IOException(e.getLocalizedMessage());
			} finally {
				if (writer != null)
					writer.close();
				writer = null;
			}
		}
		log.info("created control files for: " + nrFiles + " files");
	}

	public static class WriteMapper extends
			Mapper<Text, LongWritable, Text, Text> {

		protected FileSystem fs;
		protected String hostName;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			try {
				fs = FileSystem.get(conf);
			} catch (Exception e) {
				throw new IOException("Cannot create file system.", e);
			}
			try {
				hostName = InetAddress.getLocalHost().getHostName();
			} catch (Exception e) {
				hostName = "localhost";
			}
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			log.info("cleanup ok.");
		}

		@Override
		protected void map(Text key, LongWritable value, Context context)
				throws IOException, InterruptedException {
			String name = key.toString();
			long longValue = value.get();

			context.setStatus("starting " + name + " ::host = " + hostName);

			long tStart = System.currentTimeMillis();
			Map<String, Long> statValue = doIO(context, name, longValue);
			long tEnd = System.currentTimeMillis();
			long execTime = tEnd - tStart;
			collectStats(context, name, execTime, statValue);

			context.setStatus("finished " + name + " ::host = " + hostName);
		}

		private Map<String, Long> doIO(Context context, String name,
				long totalSize) throws IOException {
			long totalBytes = 0;
			Configuration conf = context.getConfiguration();
			// create file
			Path dst = new Path(getDataDir(conf), name);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(dst)) {
				fs.delete(dst, true);
				log.info("delete datafile: " + dst);
			}

			CompressionType compress = MRUtils.getCompressionType(conf);
			CompressionCodec codec = MRUtils.getCompressionCodec(conf);

			Option optPath = SequenceFile.Writer.file(dst);
			Option optKey = SequenceFile.Writer.keyClass(BytesWritable.class);
			Option optVal = SequenceFile.Writer.valueClass(BytesWritable.class);
			Option optCompress = SequenceFile.Writer.compression(compress,
					codec);
			SequenceFile.Writer writer = SequenceFile.createWriter(conf,
					optPath, optKey, optVal, optCompress);
			try {
				ObjectDataGenerator odg = new ObjectDataGenerator();
				odg.setup(conf);
				BytesWritable key = new BytesWritable();
				BytesWritable val = new BytesWritable();
				// write to the file
				long bufferSize = conf.getLong("bufferSize", 10000L);
				log.info("bufferSize = {}", bufferSize);
				long nrRemaining = totalSize;
				while (nrRemaining > 0) {
					if (bufferSize > nrRemaining) {
						bufferSize = nrRemaining;
					}
					for (long i = 0; i < bufferSize; i++) {
						ObjectData.ObjectBase ob = odg.genBase();
						byte[] bytes = ob.toByteArray();
						val.set(bytes, 0, bytes.length);
						totalBytes += bytes.length;

						bytes = ObjectPbfUtils.md5(ob);
						key.set(bytes, 0, bytes.length);
						totalBytes += bytes.length;

						writer.append(key, val);
					}
					context.setStatus("writing " + name + "@"
							+ (totalSize - nrRemaining) + "/" + totalSize
							+ " ::host = " + hostName);
					nrRemaining -= bufferSize;
				}
				odg.cleanup(conf);
			} finally {
				IOUtils.closeStream(writer);
			}
			Map<String, Long> statValue = new HashMap<String, Long>();
			statValue.put("records", totalSize);
			statValue.put("bytes", totalBytes);
			return statValue;
		}

		void collectStats(Context context, String name, long execTime,
				Map<String, Long> statValue) throws IOException,
				InterruptedException {
			long totalBytes = statValue.get("bytes");
			float ioRateMbSec = (float) totalBytes * 1000 / (execTime * MEGA);
			log.info("Number of bytes processed = " + totalBytes);
			log.info("Exec time = " + execTime);
			log.info("ioRateMbSec = " + ioRateMbSec);

			long totalSize = statValue.get("records");
			float ioRateRecSec = (float) totalSize * 1000 / (execTime);
			log.info("Number of records processed = " + totalSize);
			log.info("Exec time = " + execTime);
			log.info("ioRateRecSec = " + ioRateRecSec);

			context.write(new Text(AccumulatingReducer.VALUE_TYPE_LONG
					+ "tasks"), new Text(String.valueOf(1)));
			context.write(
					new Text(AccumulatingReducer.VALUE_TYPE_LONG + "time"),
					new Text(String.valueOf(execTime)));
			context.write(new Text(AccumulatingReducer.VALUE_TYPE_LONG
					+ "mb.size"), new Text(String.valueOf(totalBytes)));
			context.write(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT
					+ "mb.rate"), new Text(String.valueOf(ioRateMbSec * 1000)));
			context.write(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT
					+ "mb.sqrate"),
					new Text(String.valueOf(ioRateMbSec * ioRateMbSec * 1000)));

			context.write(new Text(AccumulatingReducer.VALUE_TYPE_LONG
					+ "rec.size"), new Text(String.valueOf(totalSize)));
			context.write(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT
					+ "rec.rate"),
					new Text(String.valueOf(ioRateRecSec * 1000)));
			context.write(
					new Text(AccumulatingReducer.VALUE_TYPE_FLOAT
							+ "rec.sqrate"),
					new Text(String.valueOf(ioRateRecSec * ioRateRecSec * 1000)));
		}
	}

}
