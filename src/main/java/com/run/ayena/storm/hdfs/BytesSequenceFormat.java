package com.run.ayena.storm.hdfs;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.storm.hdfs.bolt.format.SequenceFormat;

import backtype.storm.tuple.Tuple;

public class BytesSequenceFormat implements SequenceFormat {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4922244616592633815L;
	private transient BytesWritable key = new BytesWritable();
	private transient BytesWritable value = new BytesWritable();

	private String keyField;
	private String valueField;

	public BytesSequenceFormat(String keyField, String valueField) {
		this.keyField = keyField;
		this.valueField = valueField;
	}

	@Override
	public Class<?> keyClass() {
		return BytesWritable.class;
	}

	@Override
	public Class<?> valueClass() {
		return BytesWritable.class;
	}

	@Override
	public Writable key(Tuple tuple) {
		byte[] bytes = tuple.getBinaryByField(this.keyField);
		if (this.key == null) {
			key = new BytesWritable();
		}
		this.key.set(bytes, 0, bytes.length);
		return this.key;
	}

	@Override
	public Writable value(Tuple tuple) {
		byte[] bytes = tuple.getBinaryByField(this.valueField);
		if (this.value == null) {
			value = new BytesWritable();
		}
		this.value.set(bytes, 0, bytes.length);
		return this.value;
	}

}
