package tool.jdk;

import org.junit.Test;

public class ValueTest {

	@Test
	public void testStorage() {
		long MEGA = 0x100000L;
		System.out.println("MEGA = " + MEGA);
		long totalBytes = 500 * 1024 * 1024L;
		System.out.println("bytes = " + totalBytes);
		long execTime = 1000L;
		float ioRateMbSec = (float) totalBytes * 1000 / (execTime * MEGA);
		System.out.println("ioRateMbSec = " + ioRateMbSec);
	}

	@Test
	public void testInt() {
		System.out.println("Integer.MAX_VALUE = " + Integer.MAX_VALUE);
		System.out.println("Integer.MIN_VALUE = " + Integer.MIN_VALUE);
		int v = 100000000;
		System.out.println("v = " + v);
	}

}
