package com.zx.bigdata.bean.processdef;

import java.util.ArrayList;
import java.util.List;

/**
 * 代表报文中的一条报文记录中的一个对象（一个信息段中可能会有1-n个不同对象）
 * @author liuwu
 *
 */
public class ColumnObject {
	
	private String name;	// 该对象存贮在hbase表中对应列的列名
	private String segName;	// 该对象所在的信息段的名称(基本信息段为basic,其它可选信息段为信息类别)
	private List<SegmentDataItemPair> dataItems = new ArrayList<SegmentDataItemPair>();	// 该对象相关的所有的数据字段（来源于报文头和各个信息段的数据字段）
	
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return the dataItems
	 */
	public List<SegmentDataItemPair> getDataItems() {
		return dataItems;
	}
	/**
	 * @param dataItems the dataItems to set
	 */
	public void setDataItems(List<SegmentDataItemPair> dataItems) {
		this.dataItems = dataItems;
	}
	/**
	 * @return the segName
	 */
	public String getSegName() {
		return segName;
	}
	/**
	 * @param segName the segName to set
	 */
	public void setSegName(String segName) {
		this.segName = segName;
	}
}
