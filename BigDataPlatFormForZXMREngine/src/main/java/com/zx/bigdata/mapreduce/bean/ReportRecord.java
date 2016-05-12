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
 * 
 * @author liuwu
 *
 */
public class ReportRecord extends AbstractMap<String, List<Map<String, String>>> {

	public static final String REPORT_INFO_TYPE_COLNAME = "LITIGATIONTYPE";
	public static final String APPLICATION_SYS_CODE = "ORGINALCASENO";
	public static final String REPORT_TYPE = "ORGINALCASETYPE";
	public static final String INDICATOR_OF_FEEDBACK = "FEEDBACKSIGN";

	private Map<String, List<Map<String, String>>> record = null;
	// private Map<String, OccurrencyRateEnum> segOccurrency = null;

	public ReportRecord(DataSchema dataSchema) {
		record = new HashMap<String, List<Map<String, String>>>();
		// segOccurrency = new HashMap<String, OccurrencyRateEnum>();

		for (Segment seg : dataSchema.getSegments().values()) {
			record.put(seg.getName(), new ArrayList<Map<String, String>>());
			// segOccurrency.put(seg.getName(), seg.getOccurrencyRate());
		}

	}

	/**
	 * 获取报文信息类别
	 * 
	 * @return
	 */
	public String getReportInfoType() {
		if (record.containsKey(Segment.CONST_HEADER_KEY)) {
			return record.get(Segment.CONST_HEADER_KEY).get(0).get(REPORT_INFO_TYPE_COLNAME);
		}
		throw new RuntimeException("报文头中的报文信息类型：\"" + REPORT_INFO_TYPE_COLNAME + "\"为空。");
	}

	/**
	 * 获取应用系统代码
	 * 
	 * @return
	 */
	public String getApplicationSysCode() {
		if (record.containsKey(Segment.CONST_HEADER_KEY)) {
			return record.get(Segment.CONST_HEADER_KEY).get(0).get(APPLICATION_SYS_CODE);
		}
		throw new RuntimeException("报文头中的报文信息类型：\"" + APPLICATION_SYS_CODE + "\"为空。");
	}

	/**
	 * 获取报文类型
	 * 
	 * @return
	 */
	public String getReportType() {
		if (record.containsKey(Segment.CONST_HEADER_KEY)) {
			return record.get(Segment.CONST_HEADER_KEY).get(0).get(REPORT_TYPE);
		}
		throw new RuntimeException("报文头中的报文信息类型：\"" + REPORT_TYPE + "\"为空。");
	}

	/**
	 * 获取报文反馈标志
	 * 
	 * @return
	 */
	public String getIndicatorOfFeedback() {
		if (record.containsKey(Segment.CONST_HEADER_KEY)) {
			return record.get(Segment.CONST_HEADER_KEY).get(0).get(INDICATOR_OF_FEEDBACK);
		}
		throw new RuntimeException("报文头中的报文信息类型：\"" + INDICATOR_OF_FEEDBACK + "\"为空。");
	}

	/**
	 * 
	 * @param segName
	 * @param index
	 * @param dataItemName
	 * @return
	 */
	public String getDataItemValueFromSeg(String segName, int index, String dataItemName) {
		if (index >= record.get(segName).size()) {
			throw new RuntimeException("index is " + index + " while segment:" + segName + " has only "
					+ record.get(segName).size() + " record.");
		}
		return record.get(segName).get(index).get(dataItemName);
	}

	public void putSegValue(String segName, Map<String, String> map) {
		record.get(segName).add(map);
	}

	public void putDataItem4Seg(String segName, int index, String dataItemName, String value) {
		record.get(segName).get(index).put(dataItemName, value);
	}

	public List<Map<String, String>> put(String key, List<Map<String, String>> value) {
		return this.record.put(key, value);
	}

	@Override
	public Set<Map.Entry<String, List<Map<String, String>>>> entrySet() {
		return record.entrySet();
	}

	/**
	 * tell if the segment exists or not
	 * 
	 * @param segName
	 * @return
	 */
	public boolean isSegmentExist(String segName) {
		return record.get(segName) == null || record.get(segName).isEmpty() ? false : true;
	}

}
