package com.run.ayena.store.hadoop;

/**
 * @author Yanhong Lee
 * 
 */
public enum ObjectCounter {
	NUM_NEW_INFOS, // info表新建对象数
	NUM_UPDATE_INFOS, // info表更新对象数
	NUM_NEW_BASES, // base表新建对象数
	NUM_UPDATE_BASES, // base表更新对象数
	BYTES_READ_INFOS, // info表读取字节数
	BYTES_WRITE_INFOS, // info表写入字节数
	BYTES_READ_BASES, // base表读取字节数
	BYTES_WRITE_BASES // base表写入字节数
}
