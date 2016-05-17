package com.zx.bigdata.bean.datadef;

import java.util.ArrayList;
import java.util.List;

public class Segment {
	
	final public static String CONST_HEADER_KEY="head";	// 报文头对应的主键值
	final public static String CONST_BASIC_KEY="basic";	// 基本信息段对应的主键值
	
	private String name;	//该信息段的信息类别
	private String desc;	//对该信息段的文字描述
	private List<DataItem> dataItems = new ArrayList<DataItem>();	//该信息段包括的所有的数据字段
	private OccurrencyRateEnum occurrencyRate;	//该信息段的出现频率
	
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
	public List<DataItem> getDataItems() {
		return dataItems;
	}
	/**
	 * @param dataItems the dataItems to set
	 */
	public void setDataItems(List<DataItem> dataItems) {
		this.dataItems = dataItems;
	}
	/**
	 * @return the occurrencyRate
	 */
	public OccurrencyRateEnum getOccurrencyRate() {
		return occurrencyRate;
	}
	/**
	 * @param occurrencyRate the occurrencyRate to set
	 */
	public void setOccurrencyRate(OccurrencyRateEnum occurrencyRate) {
		this.occurrencyRate = occurrencyRate;
	}
	/**
	 * @return the desc
	 */
	public String getDesc() {
		return desc;
	}
	/**
	 * @param desc the desc to set
	 */
	public void setDesc(String desc) {
		this.desc = desc;
	}

}
