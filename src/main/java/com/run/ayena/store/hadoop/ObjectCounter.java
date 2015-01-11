package com.run.ayena.store.hadoop;

/**
 * @author Yanhong Lee
 * 
 */
public enum ObjectCounter {
	// #base表------------------
	BASE_BYTES_READ, // 读取的字节数
	BASE_BYTES_WRITE, // 写入的字节数
	BASE_BYTES_UNCHANGED, // 未变更的字节数
	BASE_NUM_NEW, // 新建的对象数
	BASE_NUM_UPDATE, // 修改的对象数
	BASE_NUM_UNCHANGED, // 未变更的对象数
	// #info表------------------
	INFO_BYTES_READ, // 读取的字节数
	INFO_BYTES_WRITE, // 写入的字节数
	INFO_BYTES_UNCHANGED, // 未变更的字节数
	INFO_NUM_NEW, // 新建的对象数
	INFO_NUM_UPDATE, // 修改的对象数
	INFO_NUM_UNCHANGED // 未变更的对象数
}
