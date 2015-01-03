package com.run.ayena.store.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yanhong Lee
 * 
 */
public class AccumulatingReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger log = LoggerFactory
			.getLogger(AccumulatingReducer.class);

	public static final String VALUE_TYPE_LONG = "l:";
	public static final String VALUE_TYPE_FLOAT = "f:";
	public static final String VALUE_TYPE_STRING = "s:";

	protected String hostName;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		log.info("Starting AccumulatingReducer !!!");
		try {
			hostName = java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			hostName = "localhost";
		}
		log.info("Starting AccumulatingReducer on " + hostName);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		log.info("Stop AccumulatingReducer on " + hostName);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String field = key.toString();

		context.setStatus("starting " + field + " ::host = " + hostName);

		// concatenate strings
		if (field.startsWith(VALUE_TYPE_STRING)) {
			StringBuffer sSum = new StringBuffer();
			for (Text value : values) {
				sSum.append(value.toString()).append(";");
			}
			context.write(key, new Text(sSum.toString()));
			log.info("finished " + field + " = " + sSum.toString());
			return;
		}
		// sum long values
		if (field.startsWith(VALUE_TYPE_FLOAT)) {
			float fSum = 0;
			for (Text value : values) {
				fSum += Float.parseFloat(value.toString());
			}
			context.write(key, new Text(String.valueOf(fSum)));
			log.info("finished " + field + " = " + fSum);
			return;
		}
		// sum long values
		if (field.startsWith(VALUE_TYPE_LONG)) {
			long lSum = 0;
			for (Text value : values) {
				lSum += Long.parseLong(value.toString());
			}
			context.write(key, new Text(String.valueOf(lSum)));
			log.info("finished " + field + " = " + lSum);
		}
		context.setStatus("finished " + field + " ::host = " + hostName);
	}

}
