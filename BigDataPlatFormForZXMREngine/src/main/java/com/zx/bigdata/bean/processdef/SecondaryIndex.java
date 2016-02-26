package com.zx.bigdata.bean.processdef;

import java.util.ArrayList;
import java.util.List;

/**
 * 当SecondaryIndex作为Subschema的属性时，其必须是追加了该subschema所对应的rowkey；<p>
 * 当SecondaryIndex作为Schema的属性时，其必然是rowkey脱离关系
 * @author liuwu
 *
 */
public class SecondaryIndex {
	
	protected List<SegmentDataItemPair> key = new ArrayList<SegmentDataItemPair>(); // 表示所有的索引关键字段
	
	// 是否是删除二级索引，每个rowkey最多只有一个删除二级索引，同时保证isAppendRowKey=true
	private boolean deleteSKey = false;
	
	private RowKeyObject rowKeyObject;	// 跟该二级索引挂钩的rowkey


	/**
	 * @return the deleteSKey
	 */
	public boolean isDeleteSKey() {
		return deleteSKey;
	}

	/**
	 * @param deleteSKey the deleteSKey to set
	 */
	public void setDeleteSKey(boolean deleteSKey) {
		this.deleteSKey = deleteSKey;
	}

	/**
	 * @return the key
	 */
	public List<SegmentDataItemPair> getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(List<SegmentDataItemPair> key) {
		this.key = key;
	}

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

}
