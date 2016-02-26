package com.zx.bigdata.mapreduce.bean;

public enum StatusCounterEnum {
	
	NUM_INPUT_RECORD,//读入报文的条数（不包括报文头和空行）
	NUM_VALID_RECORD,//读入报文中有效报文记录的条数（不包括报文头和空行）
	NUM_NORMAL_ROWKEY,//输出到HBase表中rowkey的条数（不包括二级索引）
	NUM_NORMAL_SECONDARY_KEY,//输出到HBase表中二级索引的条数
	NUM_DELETE_ROWKEY,//从hbase表中删除的rowkey的条数
	NUM_INVALID_RECORD //非法报文数据条数（不包括文件名和报文头）

}
