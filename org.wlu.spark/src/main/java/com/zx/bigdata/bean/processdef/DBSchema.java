package com.zx.bigdata.bean.processdef;

import java.util.ArrayList;
import java.util.List;

/**
 * 表示一条报文记录写入到hbase里面所需的结构信息
 * @author liuwu
 *
 */
public class DBSchema {
	
	private RowKeyObject rowKeyObject;	// 一条报文记录写入HBase的rowkey
	private List<ColumnObject> columnObjects = new ArrayList<ColumnObject>();	// 一条报文记录拆分为不同对象的集合，其中每个元素对应hbase表里面的一列
	/**
	 * @return the rowKeyObject
	 */
	public RowKeyObject getRowKeyObject() {
		return rowKeyObject;
	}
	/**
	 * @param rowKeyObject the rowKeyObject to set
	 */
	public void setRowKeyObject(RowKeyObject rowKeyObject) {
		this.rowKeyObject = rowKeyObject;
	}
	/**
	 * @return the columnObjects
	 */
	public List<ColumnObject> getColumnObjects() {
		return columnObjects;
	}
	/**
	 * @param columnObjects the columnObjects to set
	 */
	public void setColumnObjects(List<ColumnObject> columnObjects) {
		this.columnObjects = columnObjects;
	}

}
