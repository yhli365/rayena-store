package com.run.ayena.storm.hdfs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;

import backtype.storm.task.TopologyContext;

public class DateBasedFileNameFormat implements FileNameFormat {

	private static final long serialVersionUID = 2366009020150308598L;

	private String componentId;
	private int taskId;
	private String path = "/storm";
	private String prefix = "";
	private String extension = ".txt";

	private SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

	public DateBasedFileNameFormat withDateFormat(String pattern) {
		this.df = new SimpleDateFormat(pattern);
		return this;
	}

	/**
	 * Overrides the default prefix.
	 * 
	 * @param prefix
	 * @return
	 */
	public DateBasedFileNameFormat withPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	/**
	 * Overrides the default file extension.
	 * 
	 * @param extension
	 * @return
	 */
	public DateBasedFileNameFormat withExtension(String extension) {
		this.extension = extension;
		return this;
	}

	public DateBasedFileNameFormat withPath(String path) {
		this.path = path;
		return this;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext topologyContext) {
		this.componentId = topologyContext.getThisComponentId();
		this.taskId = topologyContext.getThisTaskId();
	}

	@Override
	public String getName(long rotation, long timeStamp) {
		return this.prefix + this.componentId + "-" + this.taskId + "-"
				+ rotation + "-" + timeStamp + this.extension;
	}

	public String getPath() {
		String date = df.format(new Date());
		return this.path + "/" + date;
	}

}
