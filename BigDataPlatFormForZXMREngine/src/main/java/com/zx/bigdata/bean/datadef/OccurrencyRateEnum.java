package com.zx.bigdata.bean.datadef;

/**
 * 信息段出现的频率枚举类，以M开头的，表示Mandatory;以O开头的，表示Optional
 * @author liuwu
 *
 */
public enum OccurrencyRateEnum {
	
	M_ONCE,	//必须且仅能出现一次
	M_MULTIPLE,	//出现1…n次
	O_ONCE,	//出现0…1次
	O_MULTIPLE	//出现0…n次

}
