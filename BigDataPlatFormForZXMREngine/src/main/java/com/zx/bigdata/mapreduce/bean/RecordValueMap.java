package com.zx.bigdata.mapreduce.bean;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.zx.bigdata.bean.datadef.DataSchema;
import com.zx.bigdata.bean.datadef.Segment;

/**
 * 该类维护一条报文记录，可根据信息段的名称和数据字段的名字进行写入和获取
 * @author liuwu
 *
 */
public class RecordValueMap extends AbstractMap<String, List<Map<String,String>>>{
	
	private Map<String, List<Map<String,String>>> record = null;
//	private Map<String, OccurrencyRateEnum> segOccurrency = null;
	
	public RecordValueMap(DataSchema dataSchema) {
		record = new HashMap<String, List<Map<String,String>>>();
//		segOccurrency = new HashMap<String, OccurrencyRateEnum>();
		
		for (Segment seg : dataSchema.getSegments().values()) {
			record.put(seg.getName(), new ArrayList<Map<String,String>>());
//			segOccurrency.put(seg.getName(), seg.getOccurrencyRate());
		}
		
	}
	
//	public OccurrencyRateEnum getSegOccurrencyRate(String segName) {
//		return this.segOccurrency.get(segName);
//	}
	
	public String getDataItemValueFromSeg(String segName, int index, String dataItemName) {
		return record.get(segName).get(index).get(dataItemName);
	}
	
	public void putSegValue(String segName, Map<String,String> map) {
		record.get(segName).add(map);
	}
	
	public void putDataItem4Seg(String segName, int index, String dataItemName, String value) {
		record.get(segName).get(index).put(dataItemName, value);
	}
	
	public List<Map<String,String>> put(String key, List<Map<String,String>> value) {
		return this.record.put(key, value);
	}

	@Override
	public Set<Map.Entry<String, List<Map<String,String>>>> entrySet() {
		// TODO Auto-generated method stub
		return record.entrySet();
	}


	
	

}
