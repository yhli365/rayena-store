package com.run.ayena.store.object;

import java.util.Map;

import com.google.common.collect.Maps;
import com.run.ayena.pbf.ObjectData;
import com.run.ayena.pbf.ObjectData.ObjectAttr;
import com.run.ayena.pbf.ObjectData.ObjectBase;
import com.run.ayena.store.util.ObjectUtils;

/**
 * @author Yanhong Lee
 * 
 */
public class TObjectBase {
	private static ObjectData.ObjectBase.Builder obb = ObjectData.ObjectBase
			.newBuilder();
	private static ObjectData.ObjectAttr.Builder oab = ObjectData.ObjectAttr
			.newBuilder();

	private String dataSource;
	private String protocol;
	private String action;

	private Map<String, Map<String, TObjectAttr>> attrs = Maps.newHashMap();

	/**
	 * 对象档案状态: 是否已存储
	 */
	private static final byte OAS_STORE = 0b00000001;

	/**
	 * 对象档案状态: 是否已归并
	 */
	private static final byte OAS_MERGED = 0b00000010;

	/**
	 * 对象档案状态: 是否更新
	 */
	private static final byte OAS_NEW = 0b00000100;

	private int oas = 0;

	public TObjectBase(ObjectBase ob) {
		this.dataSource = ob.getDataSource();
		this.protocol = ob.getProtocol();
		this.action = ob.getAction();
	}

	public String getDataSource() {
		return dataSource;
	}

	public String getProtocol() {
		return protocol;
	}

	public String getAction() {
		return action;
	}

	public Map<String, Map<String, TObjectAttr>> getAttrs() {
		return attrs;
	}

	/**
	 * 临时档案合并.
	 * 
	 * @param ob
	 */
	public void merge(ObjectBase ob) {
		int ts = ob.getCaptureTime();
		setOasByTs(ts);
		if (isOasMerged()) {
			for (ObjectAttr oa : ob.getPropsList()) {
				mergeAttrCounter(oa);
			}
		} else {
			Integer dv = ObjectUtils.dayValue(ts);
			for (ObjectAttr oa : ob.getPropsList()) {
				incAttrCounter(oa, ts, dv);
			}
		}
	}

	/**
	 * 未归并过的临时档案
	 * 
	 * @param oa
	 * @param ts
	 * @param dv
	 * @return
	 */
	public void incAttrCounter(ObjectAttr oa, int ts, Integer dv) {
		String code = oa.getCode();
		String value = oa.getValue();
		TObjectAttr oac;
		Map<String, TObjectAttr> vmap = attrs.get(code);
		if (vmap != null) {
			oac = vmap.get(value);
			if (oac == null) {
				oac = new TObjectAttr();
				vmap.put(value, oac);
			}
		} else {
			vmap = Maps.newHashMap();
			oac = new TObjectAttr();
			vmap.put(value, oac);
			attrs.put(code, vmap);
		}
		oac.update(ts, dv);
	}

	/**
	 * 已部分归并的档案
	 * 
	 * @param oa
	 */
	public void mergeAttrCounter(ObjectAttr oa) {
		String code = oa.getCode();
		String value = oa.getValue();
		TObjectAttr oac;
		Map<String, TObjectAttr> vmap = attrs.get(code);
		if (vmap != null) {
			oac = vmap.get(value);
			if (oac == null) {
				oac = new TObjectAttr();
				vmap.put(value, oac);
			}
		} else {
			vmap = Maps.newHashMap();
			oac = new TObjectAttr();
			vmap.put(value, oac);
			attrs.put(code, vmap);
		}
		oac.update(oa.getFirstTime(), oa.getLastTime(), oa.getCount(),
				oa.getDayCount(), oa.getDayValuesList(), oa.getDayStatsList());
	}

	/**
	 * 修改对象档案状态.
	 * 
	 * @param ts
	 */
	private void setOasByTs(int ts) {
		int toas;
		if (ts > 100) {// 临时对象
			toas = OAS_NEW;
		} else {
			toas = ts;
		}
		this.oas = toas | oas;
	}

	public boolean isOasMerged() {
		return (oas & OAS_MERGED) == OAS_MERGED;
	}

	public boolean isOasChanged() {
		return (oas & OAS_NEW) == OAS_NEW;
	}

	public boolean isOasStored() {
		return (oas & OAS_STORE) == OAS_STORE;
	}

	public static String getDimId(ObjectBase ob) {
		StringBuilder sb = new StringBuilder(ob.getDataSource());
		sb.append(ObjectUtils.sepChar).append(ob.getProtocol());
		sb.append(ObjectUtils.sepChar).append(ob.getAction());
		return sb.toString();
	}

	public byte[] toObjectBaseBytes(String type, String oid) {
		obb.clear();
		obb.setType(type);
		obb.setOid(oid);
		obb.setDataSource(dataSource);
		obb.setProtocol(protocol);
		obb.setAction(action);

		for (Map.Entry<String, Map<String, TObjectAttr>> e1 : attrs.entrySet()) {
			String code = e1.getKey();
			Map<String, TObjectAttr> values = e1.getValue();
			for (Map.Entry<String, TObjectAttr> e2 : values.entrySet()) {
				String value = e2.getKey();
				TObjectAttr toa = e2.getValue();
				oab.clear();
				oab.setCode(code);
				oab.setValue(value);
				oab.setFirstTime(toa.firstTime);
				oab.setLastTime(toa.lastTime);
				oab.setCount(toa.count);
				oab.setDayCount(toa.dayCount);
				for (Map.Entry<Integer, Integer> de : toa.dayStats.entrySet()) {
					oab.addDayValues(de.getKey());
					oab.addDayStats(de.getValue());
				}
				obb.addProps(oab.build());
			}
		}
		return obb.build().toByteArray();
	}

}
