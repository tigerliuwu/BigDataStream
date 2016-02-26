package com.zx.bigdata.bean.processdef;

import java.util.ArrayList;
import java.util.List;

/**
 * 表示一个rowkey
 * @author liuwu
 *
 */
public class RowKeyObject {
	// **************rowkey definition start ***********************
	// ********************只表示一个rowkey***********
	private List<SegmentDataItemPair> idCode = new ArrayList<SegmentDataItemPair>(); //唯一标识码
	
	private SegmentDataItemPair reportInformationType;	//该信息段数据源区分段数据字段(数据源区分段)

	private List<SegmentDataItemPair> busiKeys = new ArrayList<SegmentDataItemPair>(); //业务类型区分
	
	private SegmentDataItemPair time;//时间标识段
	// **************rowkey definition end ***********************

	/**
	 * @return the idCode
	 */
	public List<SegmentDataItemPair> getIdCode() {
		return idCode;
	}

	/**
	 * @param idCode the idCode to set
	 */
	public void setIdCode(List<SegmentDataItemPair> idCode) {
		this.idCode = idCode;
	}

	/**
	 * @return the reportInformationType
	 */
	public SegmentDataItemPair getReportInformationType() {
		return reportInformationType;
	}

	/**
	 * @param reportInformationType the reportInformationType to set
	 */
	public void setReportInformationType(SegmentDataItemPair reportInformationType) {
		this.reportInformationType = reportInformationType;
	}

	/**
	 * @return the busiKeys
	 */
	public List<SegmentDataItemPair> getBusiKeys() {
		return busiKeys;
	}

	/**
	 * @param busiKeys the busiKeys to set
	 */
	public void setBusiKeys(List<SegmentDataItemPair> busiKeys) {
		this.busiKeys = busiKeys;
	}

	/**
	 * @return the time
	 */
	public SegmentDataItemPair getTime() {
		return time;
	}

	/**
	 * @param time the time to set
	 */
	public void setTime(SegmentDataItemPair time) {
		this.time = time;
	}

}
