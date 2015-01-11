package com.run.ayena.store.object;

import java.util.Map;

import com.google.common.collect.Maps;
import com.run.ayena.pbf.ObjectData.ObjectAttr;
import com.run.ayena.pbf.ObjectData.ObjectBase;
import com.run.ayena.store.util.ObjectUtils;

/**
 * @author Yanhong Lee
 * 
 */
public class TObjectBase {

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

}
