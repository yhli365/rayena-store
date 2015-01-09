package com.run.ayena.store.object;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * @author Yanhong Lee
 *
 */
public class TObjectAttr {
	public int firstTime;
	public int lastTime;
	public int count;
	public int dayCount;
	public Map<Integer, Integer> dayStats = Maps.newHashMap();

	public TObjectAttr(int ts, Integer dv) {
		this.firstTime = ts;
		this.lastTime = ts;
		this.count = 1;
		this.dayCount = 1;
		this.dayStats.put(dv, 1);
	}

	public void update(int ts, Integer dv) {
		if (ts > this.lastTime) {
			this.lastTime = ts;
		} else if (ts < this.firstTime) {
			this.firstTime = ts;
		}
		this.count++;
		Integer v = this.dayStats.get(dv);
		if (v != null) {
			this.dayStats.put(dv, v + 1);
		} else {
			this.dayStats.put(dv, 1);
			this.dayCount++;
		}
	}
}
