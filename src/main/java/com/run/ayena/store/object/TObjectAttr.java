package com.run.ayena.store.object;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * @author Yanhong Lee
 * 
 */
public class TObjectAttr {
	public int firstTime = Integer.MAX_VALUE;
	public int lastTime = 0;
	public int count = 0;
	public int dayCount = 0;
	public Map<Integer, Integer> dayStats = Maps.newHashMap();

	public TObjectAttr() {
	}

	public void update(int ts, Integer dv) {
		if (firstTime > ts) {
			firstTime = ts;
		}
		if (lastTime < ts) {
			lastTime = ts;
		}

		count++;
		Integer v = dayStats.get(dv);
		if (v != null) {
			dayStats.put(dv, v + 1);
		} else {
			dayStats.put(dv, 1);
			dayCount++;
		}
	}

	public void update(int firstTime2, int lastTime2, int count2,
			int dayCount2, List<Integer> dayValuesList,
			List<Integer> dayStatsList) {
		if (firstTime > firstTime2) {
			firstTime = firstTime2;
		}
		if (lastTime < lastTime2) {
			lastTime = lastTime2;
		}

		count += count2;
		for (int i = 0; i < dayValuesList.size(); i++) {
			Integer dv = dayValuesList.get(i);
			Integer dc = dayStatsList.get(i);
			Integer v = dayStats.get(dv);
			if (v != null) {
				dayStats.put(dv, v + dc);
			} else {
				dayStats.put(dv, dc);
				dayCount++;
			}
		}
	}
}
