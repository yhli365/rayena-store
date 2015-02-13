package com.run.ayena.store.pbf;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.run.ayena.pbf.ObjectData.ObjectBase;
import com.run.ayena.store.util.ValueUtils;

public class ObjectDataTest {

	@Test
	public void md5Object() throws IOException {
		ObjectBase.Builder obb = ObjectBase.newBuilder();
		obb.setOid("12345678");
		obb.setType("qq.com");
		obb.setDataSource("1001");
		byte[] bytes = obb.build().toByteArray();
		String md5 = ValueUtils.md5str(bytes);
		System.out.println("md5: " + md5);

		obb.clear();
		obb.setOid("12345678");
		obb.setType("qq2.com");
		obb.setDataSource("1001");
		bytes = obb.build().toByteArray();
		String md5b = ValueUtils.md5str(bytes);
		System.out.println("md5: " + md5b);

		obb.clear();
		obb.setDataSource("1001");
		obb.setType("qq.com");
		obb.setOid("12345678");
		obb.setType("qq.com");
		bytes = obb.build().toByteArray();
		String md5c = ValueUtils.md5str(bytes);
		System.out.println("md5: " + md5c);

		Assert.assertEquals("md5 equal", md5, md5c);
	}

}
