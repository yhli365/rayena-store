package tool.jdk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class CollectionsTest {

	@Test
	public void testBinarySearch() {
		List<Integer> list = new ArrayList<Integer>();
		list.add(5);
		list.add(8);
		list.add(3);
		list.add(9);
		Collections.sort(list);
		System.out.println("[list] " + list);
		int[] keys = new int[] { 1, 3, 5, 7, 9, 12 };
		for (int key : keys) {
			List<Integer> list2 = new ArrayList<Integer>(list);
			int idx = Collections.binarySearch(list2, key);
			System.out.println("[bs] key=" + key + ", idx=" + idx);
			if (idx < 0) {
				idx = -idx - 1;
				list2.add(idx, key);
			}
			System.out.println("[list2] " + list2);

		}
	}

}
