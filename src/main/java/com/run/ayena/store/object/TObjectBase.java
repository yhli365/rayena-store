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
	private int oas = 0;
	private String dataSource;
	private String protocol;
	private String action;

	private Map<String, Map<String, TObjectAttr>> attrs = Maps.newHashMap();

	public TObjectBase(ObjectBase ob) {
		this.dataSource = ob.getDataSource();
		this.protocol = ob.getProtocol();
		this.action = ob.getAction();
	}

	public int getOas() {
		return oas;
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
		int toas = ObjectUtils.oas(ts);
		this.oas = toas | oas;
		if ((oas & ObjectUtils.OAS_MERGED) == ObjectUtils.OAS_MERGED) {
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
	public TObjectAttr incAttrCounter(ObjectAttr oa, int ts, Integer dv) {
		String code = oa.getCode();
		String value = oa.getValue();
		TObjectAttr oac;
		Map<String, TObjectAttr> vmap = attrs.get(code);
		if (vmap != null) {
			oac = vmap.get(value);
			if (oac == null) {
				oac = new TObjectAttr(ts, dv);
				vmap.put(value, oac);
			} else {
				oac.update(ts, dv);
			}
		} else {
			vmap = Maps.newHashMap();
			oac = new TObjectAttr(ts, dv);
			vmap.put(value, oac);
			attrs.put(code, vmap);
		}
		return oac;
	}

	/**
	 * 已部分归并的档案
	 * 
	 * @param oa
	 */
	public void mergeAttrCounter(ObjectAttr oa) {
		// TODO Auto-generated method stub

	}

}
