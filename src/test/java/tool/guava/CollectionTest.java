package tool.guava;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.HashMultimap;

public class CollectionTest {

	@Test
	public void testHashMultimap() {
		HashMultimap<String, String> props = HashMultimap.create();
		props.put("A", "AV1");
		props.put("B", "BV1");
		props.put("B", "BV2");
		props.put("B", "BV1");
		props.put("C", "CV1");
		props.put("C", "CV2");
		props.put("C", "CV3");
		System.out.println("[c1] = " + props);

		props.remove("A", "AV1");
		props.remove("B", "BV1");
		System.out.println("[c2] = " + props);

		for (Map.Entry<String, String> entry : props.entries()) {
			System.out
					.println("\t" + entry.getKey() + " = " + entry.getValue());
		}
	}

}
