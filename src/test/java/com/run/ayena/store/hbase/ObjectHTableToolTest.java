package com.run.ayena.store.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.run.ayena.store.StoreTestBase;

/**
 * @author Yanhong Lee
 * 
 */
public class ObjectHTableToolTest extends StoreTestBase {

	@Test
	public void createTablePerf() throws IOException {
		driver("htable -Dtype=ren -DnumRegions=5 -createTable");
	}

	@Test
	public void dropTablePerf() throws IOException {
		driver("htable -Dtype=ren -dropTable");
	}

	@Test
	public void createTable() throws IOException {
		driver("htable -Dtable.prefix=ft -Dtype=ren -DnumRegions=5 -createTable");
	}

	@Test
	public void dropTable() throws IOException {
		driver("htable -Dtable.prefix=ft -Dtype=ren -dropTable");
	}

	@Test
	public void data() throws IOException {
		driver("htable -Dtable.prefix=ft -data src/test/resources/data/object_ren.bcp");
	}

	@Test
	public void load() throws IOException {
		driver("htable -Dtable.prefix=ft -load src/test/resources/data/object_ren.bcp");
	}

	@Test
	public void qInfo() throws IOException {
		driver("htable -Dtable.prefix=ft -Dtype=ren -q.info qq.com 888001");
		driver("htable -Dtable.prefix=ft -Dtype=ren -q.info qq.com 888002");
	}

	@Test
	public void qBase() throws IOException {
		driver("htable -Dtable.prefix=ft -Dtype=ren -q.base qq.com 888001");
		driver("htable -Dtable.prefix=ft -Dtype=ren -q.base qq.com 888002 116");
	}

	@Test
	public void delete() throws IOException {
		driver("htable -Dtable.prefix=ft -Dtype=ren -delete qq.com 888001");
		driver("htable -Dtable.prefix=ft -Dtype=ren -delete qq.com 888002");
	}

	@Test
	public void testRow() throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		ByteBuffer bb = ByteBuffer.allocateDirect(4096);
		byte sep = 1;

		bb.clear();
		bb.put(Bytes.toBytes("qq.com"));
		bb.put(sep);
		bb.put(Bytes.toBytes("34665989"));

		int len = bb.position();
		byte[] infoRow = new byte[len + 2];
		bb.rewind();
		bb.get(infoRow, 2, len);

		md.update(infoRow, 2, len);
		byte[] md5 = md.digest();
		System.arraycopy(md5, 0, infoRow, 0, 2);
		System.out.println("infoRow = " + Bytes.toStringBinary(infoRow));

		bb.put(sep);
		bb.put(Bytes.toBytes("126"));
		len = bb.position();
		byte[] baseRow = new byte[bb.position() + 2];
		System.arraycopy(md5, 0, baseRow, 0, 2);
		bb.rewind();
		bb.get(baseRow, 2, len);
		System.out.println("baseRow = " + Bytes.toStringBinary(baseRow));
	}

}
