package com.zx.bigdata.bean.processdef;

/**
 * 该类用于表示一个信息段和该信息段中的一个数据字段对
 * @author liuwu
 *
 */
public class SegmentDataItemPair {
	
	private String segmentName;		// 信息段的名称即信息类别
	private String dataItemName;	// 数据字段的名字
	
	/**
	 * @return the segmentName
	 */
	public String getSegmentName() {
		return segmentName;
	}
	/**
	 * @param segmentName the segmentName to set
	 */
	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}
	/**
	 * @return the dataItemName
	 */
	public String getDataItemName() {
		return dataItemName;
	}
	/**
	 * @param dataItemName the dataItemName to set
	 */
	public void setDataItemName(String dataItemName) {
		this.dataItemName = dataItemName;
	}
}
