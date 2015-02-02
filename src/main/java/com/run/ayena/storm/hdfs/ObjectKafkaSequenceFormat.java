package com.run.ayena.storm.hdfs;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.storm.hdfs.bolt.format.SequenceFormat;

import backtype.storm.tuple.Tuple;

import com.google.protobuf.InvalidProtocolBufferException;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.store.util.ObjectPbfUtils;

public class ObjectKafkaSequenceFormat implements SequenceFormat {

	private static final long serialVersionUID = 2744694810908890578L;
	private transient BytesWritable key = new BytesWritable();
	private transient BytesWritable value = new BytesWritable();

	public ObjectKafkaSequenceFormat() {
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
		if (this.key == null) {
			key = new BytesWritable();
		}
		byte[] msg = tuple.getBinary(0);
		try {
			ObjectData.ObjectBase ob = ObjectData.ObjectBase.PARSER
					.parseFrom(msg);
			byte[] bytes = ObjectPbfUtils.md5(ob);
			this.key.set(bytes, 0, bytes.length);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return key;
	}

	@Override
	public Writable value(Tuple tuple) {
		byte[] bytes = tuple.getBinary(0);
		if (this.value == null) {
			value = new BytesWritable();
		}
		this.value.set(bytes, 0, bytes.length);
		return this.value;
	}

}
